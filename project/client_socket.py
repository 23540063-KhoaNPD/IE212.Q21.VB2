import asyncio
import websockets

async def listen():
    uri = "ws://localhost:9999"

    async with websockets.connect(uri) as websocket:
        print("🎧 Listening...")
        while True:
            data = await websocket.recv()
            print("📡 Realtime:", data)
            

asyncio.run(listen())