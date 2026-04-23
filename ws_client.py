import asyncio
import json

import websockets

from settings import (
    HEARTBEAT_INTERVAL_SEC,
    QUEUE_MAX,
    RECONNECT_DELAY_SEC,
    WS_URL,
)
from timeutils import day_str_from_ts


def _try_drop_oldest(q: asyncio.Queue):
    try:
        _ = q.get_nowait()
        q.task_done()
    except Exception:
        pass


async def _bitget_heartbeat(ws, interval_sec: int = HEARTBEAT_INTERVAL_SEC):
    while True:
        await asyncio.sleep(interval_sec)
        try:
            await ws.send("ping")
        except Exception:
            return


async def ws_receiver(inst_id: str, csv_q: asyncio.Queue):
    """
    One websocket stream per symbol.
    Only receives Bitget trade data and forwards compact rows to the CSV writer queue.
    """
    while True:
        try:
            print(f"[{inst_id}] Connecting to {WS_URL} ...")
            async with websockets.connect(
                WS_URL,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
                max_queue=1024,
            ) as ws:
                print(f"[{inst_id}] Connected, subscribing...")

                sub_msg = {
                    "op": "subscribe",
                    "args": [
                        {
                            "instType": "USDT-FUTURES",
                            "channel": "trade",
                            "instId": inst_id,
                        }
                    ],
                }
                await ws.send(json.dumps(sub_msg))

                hb_task = asyncio.create_task(_bitget_heartbeat(ws))

                try:
                    async for raw in ws:
                        if raw == "pong":
                            continue

                        if raw == "ping":
                            try:
                                await ws.send("pong")
                            except Exception:
                                pass
                            continue

                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        data = msg.get("data")
                        if not data:
                            continue

                        for t in data:
                            try:
                                ts_ms = int(t.get("ts"))
                                price = float(t.get("price"))
                            except Exception:
                                continue

                            size = t.get("size")
                            side = t.get("side")
                            trade_id = t.get("tradeId")

                            day = day_str_from_ts(ts_ms)
                            row = [ts_ms, price, size, side, trade_id]

                            if csv_q.full():
                                _try_drop_oldest(csv_q)
                            csv_q.put_nowait((inst_id, day, row))

                finally:
                    hb_task.cancel()
                    await asyncio.gather(hb_task, return_exceptions=True)

        except asyncio.CancelledError:
            print(f"[{inst_id}] Receiver cancelled, stopping.")
            raise
        except Exception as e:
            print(f"[{inst_id}] WS error, reconnecting in {RECONNECT_DELAY_SEC}s: {repr(e)}")
            await asyncio.sleep(RECONNECT_DELAY_SEC)
