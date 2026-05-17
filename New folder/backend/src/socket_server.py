import asyncio
import websockets

clients = set()

async def handler(websocket):
    print("Client connected")
    clients.add(websocket)

    try:
        async for message in websocket:
            print("Received:", message)

            # broadcast cho tất cả client
            disconnected = set()

            for client in clients:
                try:
                    await client.send(message)
                except:
                    disconnected.add(client)

            # cleanup client lỗi
            for d in disconnected:
                clients.remove(d)

    except:
        print("Client disconnected")
    finally:
        clients.remove(websocket)

async def main():
    async with websockets.serve(handler, "0.0.0.0", 9999):
        print("WebSocket running at ws://localhost:9999")
        await asyncio.Future()

asyncio.run(main())