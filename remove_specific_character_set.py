#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

try:
    import pydicom
    from pydicom.errors import InvalidDicomError
except ImportError:
    print("Missing dependency: pydicom", file=sys.stderr)
    print("Install it with: python3 -m pip install pydicom", file=sys.stderr)
    sys.exit(2)


TAG = (0x0008, 0x0005)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recursively remove DICOM SpecificCharacterSet (0008,0005)."
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=".",
        help="Root directory to scan recursively. Defaults to current directory.",
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=min(32, (os.cpu_count() or 4) * 2),
        help="Number of worker threads. Defaults to min(32, cpu_count * 2).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report matching DICOM files without modifying them.",
    )
    parser.add_argument(
        "--include-dot-underscore",
        action="store_true",
        help="Also inspect macOS ._* files. By default they are skipped.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Skip files whose path, size, and mtime match the previous run.",
    )
    parser.add_argument(
        "--state-db",
        default=None,
        help="Path to the incremental state database. Defaults to <root>/.dicom_cleanup_state.sqlite3.",
    )
    return parser.parse_args()


def iter_files(root: Path, include_dot_underscore: bool):
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if not include_dot_underscore and path.name.startswith("._"):
            continue
        yield path


class StateDB:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path))
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS file_state (
                path TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL,
                status TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def should_process(self, path: Path, size: int, mtime_ns: int) -> bool:
        row = self.conn.execute(
            "SELECT size, mtime_ns FROM file_state WHERE path = ?",
            (str(path),),
        ).fetchone()
        return row != (size, mtime_ns)

    def record(self, path: Path, size: int, mtime_ns: int, status: str) -> None:
        self.conn.execute(
            """
            INSERT INTO file_state (path, size, mtime_ns, status)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                size = excluded.size,
                mtime_ns = excluded.mtime_ns,
                status = excluded.status
            """,
            (str(path), size, mtime_ns, status),
        )

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def process_file(path: Path, dry_run: bool) -> tuple[str, Path, str | None]:
    try:
        ds = pydicom.dcmread(str(path), force=False)
    except (InvalidDicomError, OSError, PermissionError, FileNotFoundError):
        return ("non_dicom", path, None)
    except Exception as exc:
        return ("error", path, f"{type(exc).__name__}: {exc}")

    if TAG not in ds:
        return ("unchanged", path, None)

    if dry_run:
        return ("would_update", path, None)

    try:
        del ds[TAG]
        ds.save_as(str(path), write_like_original=True)
        return ("updated", path, None)
    except Exception as exc:
        return ("error", path, f"{type(exc).__name__}: {exc}")


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()

    if not root.exists():
        print(f"Root path does not exist: {root}", file=sys.stderr)
        return 2

    state_db: StateDB | None = None
    if args.incremental:
        db_path = Path(args.state_db) if args.state_db else root / ".dicom_cleanup_state.sqlite3"
        state_db = StateDB(db_path.resolve())

    counts = {
        "updated": 0,
        "would_update": 0,
        "unchanged": 0,
        "non_dicom": 0,
        "error": 0,
        "skipped_incremental": 0,
    }
    errors: list[tuple[Path, str]] = []
    scanned_files = 0
    queued_files = 0
    max_pending = max(1, args.jobs) * 4

    try:
        with ThreadPoolExecutor(max_workers=max(1, args.jobs)) as executor:
            futures: dict = {}
            for path in iter_files(root, args.include_dot_underscore):
                scanned_files += 1
                try:
                    stat = path.stat()
                except (OSError, PermissionError, FileNotFoundError) as exc:
                    counts["error"] += 1
                    errors.append((path, f"{type(exc).__name__}: {exc}"))
                    continue

                if state_db is not None and state_db.should_process(path, stat.st_size, stat.st_mtime_ns) is False:
                    counts["skipped_incremental"] += 1
                    continue

                queued_files += 1
                future = executor.submit(process_file, path, args.dry_run)
                futures[future] = path

                if len(futures) >= max_pending:
                    done, _ = wait(futures, return_when=FIRST_COMPLETED)
                    for completed in done:
                        queued_path = futures.pop(completed)
                        status, result_path, detail = completed.result()
                        counts[status] += 1
                        if status in {"updated", "would_update"}:
                            print(f"{status.upper()} {result_path}")
                        elif status == "error" and detail is not None:
                            errors.append((result_path, detail))

                        if state_db is not None and not args.dry_run and status != "error":
                            try:
                                latest_stat = queued_path.stat()
                                state_db.record(
                                    queued_path,
                                    latest_stat.st_size,
                                    latest_stat.st_mtime_ns,
                                    status,
                                )
                            except (OSError, PermissionError, FileNotFoundError):
                                pass

            while futures:
                done, _ = wait(futures, return_when=FIRST_COMPLETED)
                for completed in done:
                    queued_path = futures.pop(completed)
                    status, result_path, detail = completed.result()
                    counts[status] += 1
                    if status in {"updated", "would_update"}:
                        print(f"{status.upper()} {result_path}")
                    elif status == "error" and detail is not None:
                        errors.append((result_path, detail))

                    if state_db is not None and not args.dry_run and status != "error":
                        try:
                            latest_stat = queued_path.stat()
                            state_db.record(
                                queued_path,
                                latest_stat.st_size,
                                latest_stat.st_mtime_ns,
                                status,
                            )
                        except (OSError, PermissionError, FileNotFoundError):
                            pass

        if state_db is not None and not args.dry_run:
            state_db.commit()
    finally:
        if state_db is not None:
            state_db.close()

    print()
    print(f"Root: {root}")
    print(f"Scanned files: {scanned_files}")
    print(f"Queued files: {queued_files}")
    print(f"Skipped by incremental cache: {counts['skipped_incremental']}")
    print(f"Updated: {counts['updated']}")
    print(f"Would update: {counts['would_update']}")
    print(f"Unchanged DICOM: {counts['unchanged']}")
    print(f"Non-DICOM: {counts['non_dicom']}")
    print(f"Errors: {counts['error']}")

    if errors:
        print()
        for path, detail in errors:
            print(f"ERROR {path}: {detail}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
