import asyncio
import websockets
import json

async def listen():
    uri = "ws://localhost:9000"

    async with websockets.connect(uri) as websocket:
        while True:
            data = await websocket.recv()
            weather = json.loads(data)
            print("Weather:", weather)

asyncio.run(listen())