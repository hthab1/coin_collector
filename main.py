import asyncio

from csv_sink import csv_writer_worker
from settings import INST_IDS, QUEUE_MAX
from ws_client import ws_receiver


async def main():
    csv_q = asyncio.Queue(maxsize=QUEUE_MAX)

    writer_task = asyncio.create_task(csv_writer_worker(csv_q))
    receiver_tasks = [
        asyncio.create_task(ws_receiver(inst_id, csv_q))
        for inst_id in INST_IDS
    ]

    try:
        await asyncio.gather(*receiver_tasks)
    except KeyboardInterrupt:
        print("Ctrl+C, shutting down collector...")
    finally:
        for task in receiver_tasks:
            task.cancel()
        await asyncio.gather(*receiver_tasks, return_exceptions=True)

        try:
            csv_q.put_nowait("__STOP__")
        except Exception:
            pass
        await writer_task


if __name__ == "__main__":
    asyncio.run(main())
