import asyncio
import json
import aiohttp
import websockets

API_KEY = "API_KEY"
LOCATION = "Da Lat"
PORT = 9000

async def get_weather():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://api.weatherapi.com/v1/current.json?key={API_KEY}&q={LOCATION}"
            ) as res:
                data = await res.json()

                return {
                    "city": data["location"]["name"],
                    "temp": data["current"]["temp_c"],
                    "humidity": data["current"]["humidity"],
                    "condition": data["current"]["condition"]["text"],
                }
    except Exception as e:
        print("Error:", e)
        return None

async def handler(websocket):
    print("Client connected")

    try:
        while True:
            weather = await get_weather()
            if weather:
                await websocket.send(json.dumps(weather))
            await asyncio.sleep(5)
    except:
        print("Client disconnected")

async def main():
    async with websockets.serve(handler, "0.0.0.0", PORT):
        print(f"Server running at ws://localhost:{PORT}")
        await asyncio.Future()

asyncio.run(main())