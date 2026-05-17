import asyncio
import websockets

clients = set()

async def handler(websocket):
    print("Client connected")
    clients.add(websocket)

    try:
        async for message in websocket:
            print("Received message:", message)
            disconnected = set()

            for client in clients:
                try:
                    await client.send(message)
                except Exception:
                    disconnected.add(client)

            for client in disconnected:
                clients.discard(client)

    except websockets.exceptions.ConnectionClosedOK:
        pass
    except Exception as e:
        print("WebSocket handler error:", e)
    finally:
        clients.discard(websocket)
        print("Client disconnected")

async def main():
    async with websockets.serve(handler, "0.0.0.0", 9999):
        print("WebSocket server running on ws://0.0.0.0:9999")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
