# scratch/test_binance.py
import asyncio
import json
import websockets

URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

async def main():
    async with websockets.connect(URL) as ws:
        for _ in range(5):  # print 5 events then exit
            msg = await ws.recv()
            data = json.loads(msg)
            print({
                "symbol":   data["s"],
                "price":    data["p"],
                "quantity": data["q"],
                "trade_id": data["t"],
                "event_ts": data["T"],   # exchange timestamp
                "recv_ts":  data["E"],   # when Binance sent it to you
            })

asyncio.run(main())