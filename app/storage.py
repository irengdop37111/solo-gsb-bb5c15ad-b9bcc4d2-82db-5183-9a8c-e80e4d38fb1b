"""On-disk layout and merge logic for chunk payloads."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import sqlite3
import threading
import time
import uuid
import zipfile
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

from . import config, db

_COPY_BUFFER = 1024 * 1024  # 1 MiB streaming buffer for merge/verify/archive IO

# How often a same-key request that lost the builder race re-checks the DB for
# the builder's outcome. Cross-process waiters poll regardless; in-process
# waiters are also woken immediately via _idempotency_events.
_IDEMPOTENCY_POLL_INTERVAL = 0.05

log = logging.getLogger("chunked-upload")

# Guards against concurrent merges of the same session. The idle-session
# reaper also takes (non-blocking) this lock, so a session mid-merge can
# never be expired underneath it.
_merge_locks: dict[str, threading.Lock] = {}
_merge_locks_guard = threading.Lock()


def merge_lock(session_id: str) -> threading.Lock:
    with _merge_locks_guard:
        return _merge_locks.setdefault(session_id, threading.Lock())


class SessionExpired(Exception):
    """Raised when an operation targets an expired session.

    Carries the recorded expiry timestamp for the 410 response body.
    """

    def __init__(self, expired_at: str | None):
        super().__init__("upload session has expired")
        self.expired_at = expired_at


def sanitize_filename(name: str) -> str:
    """Reduce a client-supplied filename to a safe path component."""
    base = os.path.basename(name.strip().replace(" ", "_"))
    return re.sub(r"[^A-Za-z0-9._-]", "_", base).strip("._") or "file"


def session_dir(session_id: str) -> Path:
    return config.sessions_dir() / session_id


def chunks_dir(session_id: str) -> Path:
    return session_dir(session_id) / "chunks"


def chunk_path(session_id: str, index: int) -> Path:
    return chunks_dir(session_id) / f"{index:08d}.part"


def expected_chunk_size(session: sqlite3.Row, index: int) -> int:
    """All chunks are `chunk_size` bytes except the last, which takes the remainder."""
    if index == session["total_chunks"] - 1:
        return session["total_size"] - session["chunk_size"] * (session["total_chunks"] - 1)
    return session["chunk_size"]


def final_path_for(session: sqlite3.Row) -> Path:
    name = f"{session['id']}_{sanitize_filename(session['filename'])}"
    return config.files_dir() / name


def iter_file_range(path: Path, start: int, end: int) -> Iterator[bytes]:
    """Yield the inclusive byte range [start, end] of `path` in bounded blocks.

    Reads are capped at _COPY_BUFFER, so even a huge range is streamed
    without ever loading the whole file into memory.
    """
    remaining = end - start + 1
    with open(path, "rb") as fh:
        fh.seek(start)
        while remaining > 0:
            block = fh.read(min(_COPY_BUFFER, remaining))
            if not block:
                break  # file shrank underneath us; stop cleanly
            remaining -= len(block)
            yield block


def reconcile() -> None:
    """Drop chunk rows whose payload file vanished (crash between DB and disk).

    Runs at startup so a restarted service reports exactly the chunks that are
    physically present; clients simply re-upload the rest.
    """
    with db.connect() as conn:
        sessions = conn.execute(
            "SELECT * FROM sessions WHERE status = 'uploading'"
        ).fetchall()
        for session in sessions:
            rows = conn.execute(
                "SELECT chunk_index, size FROM chunks WHERE session_id = ?",
                (session["id"],),
            ).fetchall()
            for row in rows:
                path = chunk_path(session["id"], row["chunk_index"])
                if not path.is_file() or path.stat().st_size != row["size"]:
                    conn.execute(
                        "DELETE FROM chunks WHERE session_id = ? AND chunk_index = ?",
                        (session["id"], row["chunk_index"]),
                    )
    reconcile_archives()


def reconcile_archives() -> None:
    """Heal crashes around the temp-file -> publish -> record window.

    Removes published archive files with no DB row, DB rows with no file, their
    entry rows, and any leftover temp artifacts, so a failed process can never
    leave a downloadable half-built ZIP behind. Idempotency keys stranded in
    'building' by a killed builder are released too: the key must never stay
    owned by a build that no longer exists.
    """
    files_dir = config.archives_dir()
    with db.connect() as conn:
        ids = {
            row["id"] for row in conn.execute("SELECT id FROM archives").fetchall()
        }
        on_disk = (
            {p.stem for p in files_dir.glob("*.zip")} if files_dir.is_dir() else set()
        )

        for archive_id in ids - on_disk:
            conn.execute("DELETE FROM archives WHERE id = ?", (archive_id,))
        for stale in on_disk - ids:
            (files_dir / f"{stale}.zip").unlink(missing_ok=True)

        # Building rows predate this boot, so their builder process is gone by
        # definition. Rows that happen to point at a now-reconciled archive go
        # with the archive row (FK cascade); the rest are released outright.
        conn.execute(
            "DELETE FROM archive_idempotency WHERE status = 'building'"
        )

    # Temporary artifacts from an interrupted build (none have a DB record).
    if files_dir.is_dir():
        for tmp in files_dir.glob("*.zip.tmp-*"):
            tmp.unlink(missing_ok=True)


def expire_idle_sessions(now: datetime | None = None) -> list[str]:
    """Expire uploading sessions whose idle lease has elapsed.

    A session is marked `expired` and has its chunk rows/dir removed only if it
    is still `uploading` *and* still idle at the moment of the UPDATE — a
    chunk upload or status query racing the sweep refreshes `last_activity`
    and makes the guarded UPDATE match zero rows, so active sessions are
    never touched. Completed sessions are skipped outright, as are sessions
    currently being merged (the merge lock is taken non-blocking). Returns the
    list of expired session ids.
    """
    moment = now or datetime.now(timezone.utc)
    expired: list[str] = []
    with db.connect() as conn:
        candidates = conn.execute(
            "SELECT * FROM sessions WHERE status = 'uploading'"
        ).fetchall()
        stale = [s for s in candidates if db.lease_expired(s, moment)]

    for session in stale:
        sid = session["id"]
        if not merge_lock(sid).acquire(blocking=False):
            continue  # a merge is in progress; it beats the reaper
        try:
            moment_ts = moment.isoformat(timespec="milliseconds")
            with db.connect() as conn:
                cur = conn.execute(
                    "UPDATE sessions SET status = 'expired', expired_at = ? "
                    "WHERE id = ? AND status = 'uploading' AND last_activity = ?",
                    (moment_ts, sid, session["last_activity"]),
                )
                if cur.rowcount != 1:
                    continue  # refreshed or completed since the SELECT
                conn.execute("DELETE FROM chunks WHERE session_id = ?", (sid,))
            shutil.rmtree(session_dir(sid), ignore_errors=True)
            expired.append(sid)
            log.info("session %s expired idle lease at %s", sid, moment_ts)
        finally:
            merge_lock(sid).release()
    return expired


class MergeError(Exception):
    """Raised when chunks are not mergeable; carries the refreshed missing list."""

    def __init__(self, message: str, missing: list[int]):
        super().__init__(message)
        self.missing = missing


def merge_session(session_id: str) -> dict:
    """Verify and atomically merge all chunks into the final file.

    Every chunk's SHA-256 is re-verified against the DB record while streaming
    to the target; the merged file appears via os.replace() (atomic on POSIX),
    so observers only ever see a complete file or nothing.
    """
    with db.connect() as conn:
        session = db.get_session(conn, session_id)
        if session is None:
            raise KeyError(session_id)
        if session["status"] == "completed":
            return db.session_payload(conn, session)
        if session["status"] == "expired":
            raise SessionExpired(session["expired_at"])
        missing = db.missing_indexes(conn, session)
        if missing:
            raise MergeError(f"{len(missing)} chunk(s) still missing", missing)
        rows = conn.execute(
            "SELECT chunk_index, size, sha256 FROM chunks "
            "WHERE session_id = ? ORDER BY chunk_index",
            (session_id,),
        ).fetchall()

    final_path = final_path_for(session)
    tmp_path = final_path.with_name(final_path.name + ".tmp")
    overall = hashlib.sha256()
    corrupt: list[int] = []
    try:
        with open(tmp_path, "wb") as out:
            for row in rows:
                path = chunk_path(session_id, row["chunk_index"])
                if not path.is_file():
                    corrupt.append(row["chunk_index"])
                    continue
                hasher = hashlib.sha256()
                with open(path, "rb") as part:
                    while True:
                        buf = part.read(_COPY_BUFFER)
                        if not buf:
                            break
                        hasher.update(buf)
                        overall.update(buf)
                        out.write(buf)
                if hasher.hexdigest() != row["sha256"]:
                    corrupt.append(row["chunk_index"])
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise

    if corrupt:
        # Payload diverged from its recorded digest (e.g. disk corruption):
        # forget those chunks so the client can re-upload them.
        tmp_path.unlink(missing_ok=True)
        with db.connect() as conn:
            for index in corrupt:
                conn.execute(
                    "DELETE FROM chunks WHERE session_id = ? AND chunk_index = ?",
                    (session_id, index),
                )
                chunk_path(session_id, index).unlink(missing_ok=True)
            session = db.get_session(conn, session_id)
            missing = db.missing_indexes(conn, session)
        raise MergeError(
            f"{len(corrupt)} chunk(s) failed re-verification and were discarded",
            missing,
        )

    digest = overall.hexdigest()
    declared = session["file_sha256"]
    if declared and declared.lower() != digest:
        tmp_path.unlink(missing_ok=True)
        raise MergeError(
            f"merged file sha256 {digest} does not match declared {declared}",
            missing=[],
        )

    os.replace(tmp_path, final_path)  # atomic publish

    with db.connect() as conn:
        # Status guard: the reaper cannot reach a session holding the merge
        # lock, but never overwrite an expired/completed row regardless.
        cur = conn.execute(
            "UPDATE sessions SET status = 'completed', final_path = ?, "
            "file_sha256 = ?, updated_at = ? WHERE id = ? AND status = 'uploading'",
            (str(final_path), digest, db.utcnow(), session_id),
        )
        if cur.rowcount != 1:
            conn.rollback()
            final_path.unlink(missing_ok=True)
            shutil.rmtree(session_dir(session_id), ignore_errors=True)
            refreshed = db.get_session(conn, session_id)
            if refreshed is not None and refreshed["status"] == "expired":
                raise SessionExpired(refreshed["expired_at"])
            raise MergeError("session is no longer uploading", missing=[])
        conn.execute("DELETE FROM chunks WHERE session_id = ?", (session_id,))
        session = db.get_session(conn, session_id)
    shutil.rmtree(session_dir(session_id), ignore_errors=True)
    with db.connect() as conn:
        session = db.get_session(conn, session_id)
        return db.session_payload(conn, session)


# ---------------------------------------------------------------- archives


class ArchiveSourceError(Exception):
    """A requested source session cannot feed an archive.

    `kind` is "not_found" (unknown session id), "not_completed" (the session is
    still uploading or expired) or "missing" (the merged product vanished or
    changed); the offending session id is carried alongside.
    """

    def __init__(self, kind: str, session_id: str | None = None):
        super().__init__(f"archive source is {kind}")
        self.kind = kind
        self.session_id = session_id


def _zip_entry_name(filename: str) -> str:
    """Reduce a client filename to a safe ZIP member name.

    Only the final path component survives (entries never escape the archive
    root) and the extension (last suffix) is preserved.
    """
    name = sanitize_filename(filename)
    stem, dot, suffix = name.rpartition(".")
    stem = re.sub(r"[^A-Za-z0-9._-]", "_", stem if dot else suffix).strip("._")
    stem = stem or "file"
    return stem + ("." + suffix if dot and suffix else "")


def unique_entry_names(sessions: list[sqlite3.Row]) -> list[str]:
    """Assign stable, collision-free ZIP member names in request order.

    The first occurrence keeps its name; later ones (compared
    case-insensitively, so the ZIP stays safe on every extractor/OS) get a
    `-<n>` suffix on the stem while the extension is retained.
    """
    used: set[str] = set()
    names: list[str] = []
    for session in sessions:
        candidate = _zip_entry_name(session["filename"])
        if candidate.lower() in used:
            stem, dot, suffix = candidate.rpartition(".")
            stem = stem if dot else suffix
            suffix = ("." + suffix) if dot else ""
            counter = 2
            while True:
                candidate = f"{stem}-{counter}{suffix}"
                if candidate.lower() not in used:
                    break
                counter += 1
        used.add(candidate.lower())
        names.append(candidate)
    return names


def archive_path(archive_id: str) -> Path:
    return config.archives_dir() / f"{archive_id}.zip"


class IdempotencyConflict(Exception):
    """An Idempotency-Key was reused with a different ordered session list.

    HTTP 409: the key permanently represents one request content until the
    archive it produced is deleted.
    """

    def __init__(self, key: str):
        super().__init__("idempotency key was already used with a different request")
        self.key = key


class ArchiveInProgress(Exception):
    """Another request with the same key still owns the build.

    HTTP 202: the caller waited past its deadline without a terminal result and
    should retry the identical request.
    """

    def __init__(self, key: str):
        super().__init__("an archive with this idempotency key is being built")
        self.key = key


# Per-key in-process signals: the builder sets its Event when it reaches a
# terminal state, waking same-process waiters immediately; cross-process
# waiters (and the race where the Event was cleared before a waiter registered)
# fall back to the short DB poll.
_idempotency_events: dict[str, threading.Event] = {}
_idempotency_events_guard = threading.Lock()


def _idempotency_event(key: str) -> threading.Event:
    with _idempotency_events_guard:
        event = _idempotency_events.get(key)
        if event is None:
            event = threading.Event()
            _idempotency_events[key] = event
        return event


def _discard_idempotency_event(key: str, event: threading.Event) -> None:
    """Builder-only cleanup: retire the Event it created for this cycle."""
    with _idempotency_events_guard:
        if _idempotency_events.get(key) is event:
            _idempotency_events.pop(key, None)


def request_fingerprint(session_ids: list[str]) -> tuple[str, str]:
    """(hash, canonical json) of the ordered session id list.

    The list is the complete request content of POST /archives, so its order
    and membership fully define the fingerprint: the same key with a different
    list (or the same ids in a different order) is a conflict.
    """
    content = json.dumps(session_ids, separators=(",", ":"), ensure_ascii=False)
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    return digest, content


def _record_archive(conn: sqlite3.Connection, archive_id: str, published: Path,
                    members: list[dict], moment: str) -> None:
    conn.execute(
        "INSERT INTO archives (id, path, size, created_at) "
        "VALUES (?, ?, ?, ?)",
        (archive_id, str(published), published.stat().st_size, moment),
    )
    for position, member in enumerate(members):
        conn.execute(
            "INSERT INTO archive_entries "
            "(archive_id, position, session_id, entry_name, file_size) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                archive_id,
                position,
                member["session_id"],
                member["entry_name"],
                member["file_size"],
            ),
        )


def _build_archive(session_ids: list[str], idempotency_key: str | None = None) -> dict:
    """Build a ZIP from completed sessions' products, in the given order.

    Every source file is opened and validated *first*; only then is the
    temporary archive written in bounded blocks and published atomically with
    os.replace(). For an unkeyed request the DB row is then committed on its
    own; for a keyed request the archive row and the key's flip to 'completed'
    are committed in one transaction, so a crash can never leave a key stuck
    'building' while a downloadable archive exists. Any failure before publish
    removes the temp file, so a broken archive is never downloadable.
    """
    sources: list[sqlite3.Row] = []
    with db.connect() as conn:
        for sid in session_ids:
            session = db.get_session(conn, sid)
            if session is None:
                raise ArchiveSourceError("not_found", sid)
            if session["status"] != "completed" or not session["final_path"]:
                raise ArchiveSourceError("not_completed", sid)
            sources.append(session)
        names = unique_entry_names(sources)

    # Phase 1: open and pin every source file before touching the archive.
    # A deletion racing us cannot remove an open file's bytes, and a vanished
    # or truncated product fails the whole request before any ZIP is written.
    members: list[dict] = []
    handles: list = []
    try:
        for session, name in zip(sources, names):
            try:
                fh = open(session["final_path"], "rb")
            except OSError:
                raise ArchiveSourceError("missing", session["id"])
            handles.append(fh)
            size = fh.seek(0, os.SEEK_END)
            if size != session["total_size"]:
                # The product diverged from the metadata the session holds.
                raise ArchiveSourceError("missing", session["id"])
            fh.seek(0)
            members.append(
                {
                    "session_id": session["id"],
                    "entry_name": name,
                    "file_size": size,
                }
            )

        # Phase 2: stream the pinned handles into a temp archive in bounded
        # blocks, publish it atomically, then record it. Nothing in this window
        # is reachable through the download endpoint.
        archive_id = uuid.uuid4().hex
        config.archives_dir().mkdir(parents=True, exist_ok=True)
        published = archive_path(archive_id)
        tmp_path = published.with_name(
            published.name + f".tmp-{uuid.uuid4().hex}"
        )
        try:
            with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_STORED) as archive:
                for fh, member in zip(handles, members):
                    info = zipfile.ZipInfo(
                        filename=member["entry_name"],
                        date_time=(1980, 1, 1, 0, 0, 0),
                    )
                    info.create_system = 3  # portable Unix-style entries
                    info.external_attr = (0o644 & 0xFFFF) << 16
                    info.compress_type = zipfile.ZIP_STORED
                    info.file_size = member["file_size"]
                    written = 0
                    # ZIP64 is only needed for an entry at/above the 4 GiB limit.
                    with archive.open(
                        info,
                        "w",
                        force_zip64=member["file_size"] >= zipfile.ZIP64_LIMIT,
                    ) as out:
                        while True:
                            block = fh.read(_COPY_BUFFER)
                            if not block:
                                break
                            out.write(block)
                            written += len(block)
                    if written != member["file_size"]:
                        raise OSError("source file changed while archiving")

            os.replace(tmp_path, published)  # atomic publish
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
    finally:
        for fh in handles:
            fh.close()

    moment = db.utcnow()
    with db.connect() as conn:
        try:
            _record_archive(conn, archive_id, published, members, moment)
            if idempotency_key is not None:
                # Same transaction as the archive row: the key is 'completed'
                # exactly when the archive becomes downloadable.
                cur = conn.execute(
                    "UPDATE archive_idempotency SET status = 'completed', "
                    "archive_id = ?, updated_at = ? WHERE key = ? AND status = 'building'",
                    (archive_id, moment, idempotency_key),
                )
                if cur.rowcount != 1:
                    raise RuntimeError("idempotency key is no longer held by this build")
            conn.commit()
        except BaseException:
            conn.rollback()
            published.unlink(missing_ok=True)
            raise

    with db.connect() as conn:
        return archive_payload(conn, db.get_archive(conn, archive_id))


def create_archive(session_ids: list[str]) -> dict:
    """Unkeyed archive build: the original, stateless POST /archives behavior."""
    return _build_archive(session_ids)


def _release_building_key(key: str) -> None:
    """Delete a key still owned by us after a failed/aborted build.

    Guarded on status='building' so a row that somehow reached 'completed' is
    never torn down. The next identical request becomes a fresh build, so a
    failure can never permanently occupy the key.
    """
    with db.connect() as conn:
        conn.execute(
            "DELETE FROM archive_idempotency WHERE key = ? AND status = 'building'",
            (key,),
        )
        conn.commit()


def _wait_for_idempotent_result(key: str, request_hash: str,
                                event: threading.Event,
                                deadline: float) -> dict | None:
    """Follow another builder for `key` until it finishes or `deadline` passes.

    Returns the stored archive payload on success, None if the builder released
    the key (failure) or its record vanished (caller races a fresh claim).
    Raises IdempotencyConflict on a different request content and
    ArchiveInProgress if the builder has not finished in time.
    """
    while True:
        with db.connect() as conn:
            row = db.get_idempotency(conn, key)
            if row is None:
                # The builder released its key (failure) or the record was
                # otherwise lost: caller races to claim a fresh build.
                return None
            if row["status"] == "building":
                if row["request_hash"] != request_hash:
                    # Can only happen via direct DB meddling; stay honest.
                    raise IdempotencyConflict(key)
            else:
                if row["request_hash"] != request_hash:
                    raise IdempotencyConflict(key)
                archive = db.get_archive(conn, row["archive_id"])
                if archive is not None:
                    return archive_payload(conn, archive)
                # Key says completed but the archive is gone (normally the
                # delete cascades the key away): clear the orphaned row so the
                # caller's next claim attempt succeeds instead of looping.
                conn.execute(
                    "DELETE FROM archive_idempotency WHERE key = ?", (key,)
                )
                conn.commit()
                return None

        if time.monotonic() >= deadline:
            raise ArchiveInProgress(key)
        event.wait(_IDEMPOTENCY_POLL_INTERVAL)


def create_idempotent_archive(
    session_ids: list[str],
    key: str,
    wait_timeout: float | None = None,
) -> tuple[dict, bool]:
    """Build or replay a ZIP archive bound to an Idempotency-Key.

    Returns ``(payload, replayed)``. Semantics:

    * key unseen for this request content -> one builder builds (201).
    * same key + identical ordered list while building -> wait; the builder's
      result is replayed (200), or 202 (ArchiveInProgress) past the timeout.
    * same key + identical list after success -> the same archive is returned,
      never rebuilt, even if the source sessions were deleted meanwhile.
    * same key + different list/order -> 409 (IdempotencyConflict).
    * a failed/interrupted build releases the key; the retry builds anew.
    """
    request_hash, request_json = request_fingerprint(session_ids)
    timeout = (
        config.IDEMPOTENCY_WAIT_TIMEOUT_SECONDS
        if wait_timeout is None
        else wait_timeout
    )
    deadline = time.monotonic() + timeout

    while True:
        event = _idempotency_event(key)
        is_builder = False
        orphan_cleared = False
        with db.connect() as conn:
            existing = db.get_idempotency(conn, key)
            if existing is not None:
                if existing["request_hash"] != request_hash:
                    raise IdempotencyConflict(key)
                if existing["status"] == "completed":
                    archive = db.get_archive(conn, existing["archive_id"])
                    if archive is not None:
                        return archive_payload(conn, archive), True
                    # The archive vanished despite a completed key (normally the
                    # delete cascades the key away): drop the orphaned row in
                    # the same txn and fall through to a fresh claim.
                    conn.execute(
                        "DELETE FROM archive_idempotency WHERE key = ?", (key,)
                    )
                    conn.commit()
                    orphan_cleared = True
                # else: another in-process or prior-process builder owns it.
            else:
                moment = db.utcnow()
                try:
                    conn.execute(
                        "INSERT INTO archive_idempotency "
                        "(key, status, request_hash, request_json, archive_id, "
                        "created_at, updated_at) VALUES (?, 'building', ?, ?, NULL, ?, ?)",
                        (key, request_hash, request_json, moment, moment),
                    )
                    conn.commit()
                    is_builder = True
                except sqlite3.IntegrityError:
                    conn.rollback()

        if is_builder:
            event = _idempotency_event(key)
            try:
                return _build_archive(session_ids, idempotency_key=key), False
            except BaseException:
                _release_building_key(key)
                raise
            finally:
                event.set()
                _discard_idempotency_event(key, event)

        if orphan_cleared:
            # No live builder to follow; the next loop iteration claims anew.
            continue

        # Lost the claim race (or the record we saw vanished): wait for the
        # current builder, then either replay or make one fresh claim attempt.
        result = _wait_for_idempotent_result(key, request_hash, event, deadline)
        if result is not None:
            return result, True
        # Key was released or invalidated; retry the claim only if time remains.
        if time.monotonic() >= deadline:
            raise ArchiveInProgress(key)


def archive_payload(conn: sqlite3.Connection, archive: sqlite3.Row) -> dict:
    entries = db.get_archive_entries(conn, archive["id"])
    return {
        "archive_id": archive["id"],
        "size": archive["size"],
        "created_at": archive["created_at"],
        "files": [
            {
                "position": row["position"],
                "session_id": row["session_id"],
                "entry_name": row["entry_name"],
                "size": row["file_size"],
            }
            for row in entries
        ],
    }


def delete_archive(archive_id: str) -> bool:
    """Remove an archive's record and its ZIP. Returns False if unknown."""
    with db.connect() as conn:
        archive = db.get_archive(conn, archive_id)
        if archive is None:
            return False
        path = Path(archive["path"])
        conn.execute("DELETE FROM archives WHERE id = ?", (archive_id,))
    # The record (and its entries, via FK cascade) is gone first: a concurrent
    # download that already resolved the path may still finish, but nobody can
    # start a new one against a deleted archive.
    path.unlink(missing_ok=True)
    return True
