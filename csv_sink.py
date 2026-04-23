import asyncio
import csv
from pathlib import Path
from typing import List, Optional, Tuple

from settings import BATCH_SIZE, CSV_DIR, FLUSH_EVERY_SEC

RowItem = Tuple[str, str, List]  # (inst_id, day_str, [ts, price, size, side, trade_id])


def csv_path_for_day(inst_id: str, day: str) -> Path:
    return CSV_DIR / f"{inst_id}_{day}.csv"


def ensure_csv_header(path: Path):
    if not path.exists() or path.stat().st_size == 0:
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["ts", "price", "size", "side", "trade_id"])


def _flush_rows(path: Path, rows: List[List]):
    ensure_csv_header(path)
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows)


async def csv_writer_worker(q: asyncio.Queue):
    """
    Consumes rows from queue and writes to per-symbol, per-day CSV files.
    Queue items must be: (inst_id, day_str, row_list) or the sentinel "__STOP__".
    """
    CSV_DIR.mkdir(parents=True, exist_ok=True)

    current_key: Optional[tuple[str, str]] = None
    current_path: Optional[Path] = None
    buffer: List[List] = []

    loop = asyncio.get_event_loop()
    last_flush = loop.time()

    while True:
        try:
            item = await asyncio.wait_for(q.get(), timeout=FLUSH_EVERY_SEC)
        except asyncio.TimeoutError:
            item = None

        now = loop.time()

        if item is None:
            if buffer and current_path and (now - last_flush) >= FLUSH_EVERY_SEC:
                _flush_rows(current_path, buffer)
                buffer.clear()
                last_flush = now
            continue

        if item == "__STOP__":
            break

        inst_id, day, row = item  # type: ignore
        incoming_key = (inst_id, day)

        if current_key != incoming_key:
            if buffer and current_path:
                _flush_rows(current_path, buffer)
                buffer.clear()

            current_key = incoming_key
            current_path = csv_path_for_day(inst_id, day)
            ensure_csv_header(current_path)
            last_flush = now

        buffer.append(row)

        if len(buffer) >= BATCH_SIZE and current_path:
            _flush_rows(current_path, buffer)
            buffer.clear()
            last_flush = now

        q.task_done()

    if buffer and current_path:
        _flush_rows(current_path, buffer)
