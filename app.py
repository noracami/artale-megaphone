import asyncio
import websockets
import json
import os
import signal
import sys
import psycopg2
from psycopg2.extras import execute_values

# --- 環境變數設定 ---
PG_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", 25432)),
    "database": os.getenv("PG_DATABASE", ""),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}

WS_URL = os.getenv("WS_URL", "")
# Time in seconds to wait before attempting to reconnect the WebSocket
_delay_val_str = os.getenv("WS_RECONNECT_DELAY", "5")  # Default to string "5"
RECONNECT_DELAY = 5  # Default value if parsing fails or value is invalid
try:
    parsed_delay = int(_delay_val_str)
    if parsed_delay > 0:
        RECONNECT_DELAY = parsed_delay
    else:
        # Log warning if non-positive and use the default
        print(f"[WARN] WS_RECONNECT_DELAY ('{_delay_val_str}') must be positive. Using default {RECONNECT_DELAY}s.")
except ValueError:
    # Log warning if not an int and use the default
    print(f"[WARN] WS_RECONNECT_DELAY ('{_delay_val_str}') is not a valid integer. Using default {RECONNECT_DELAY}s.")

# --- 全域變數 ---
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
websocket = None


def store_messages(messages):
    if not messages:
        return

    def split_user(raw):
        if "#" not in raw:
            return raw, None
        return raw.split("#", 1)

    try:
        conn = psycopg2.connect(**PG_CONFIG)
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO chat_messages (username, profile_code, text, channel, timestamp)
                VALUES %s
                """,
                [
                    (
                        *split_user(m.get("username")),  # -> username, profile_code
                        m.get("text"),
                        m.get("channel"),
                        m.get("timestamp"),
                    )
                    for m in messages
                ],
            )
        conn.commit()
        conn.close()
        print(f"[DB] Inserted {len(messages)} message(s)")

    except Exception as e:
        print("[DB ERROR]", str(e))


async def listen_forever():
    """Continuously listen to the WebSocket and auto reconnect on failure."""
    global websocket
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                websocket = ws
                print("[INFO] Connected to", WS_URL)

                await ws.send(json.dumps({"type": "ping"}))
                print("[SEND] ping")

                while True:
                    try:
                        message = await ws.recv()
                        data = json.loads(message)

                        if isinstance(data, list):
                            store_messages(data)
                        else:
                            print("[RECV SINGLE]", data)

                    except websockets.ConnectionClosed:
                        print(f"[WARN] WebSocket connection closed. Reconnecting in {RECONNECT_DELAY}s...")
                        websocket = None
                        break
                    except Exception as e:
                        print("[ERROR]", str(e))
                        websocket = None
                        break
        except Exception as e:
            print("[FATAL ERROR]", str(e))
            websocket = None
        await asyncio.sleep(RECONNECT_DELAY)


# --- Shutdown handler ---
def shutdown_handler(sig, frame):
    print("\n[INFO] Shutting down gracefully...")
    if websocket and websocket.close_code is None:
        loop.create_task(websocket.close())
    loop.create_task(shutdown())


async def shutdown():
    await asyncio.sleep(0.2)
    loop.stop()


# --- 註冊訊號處理器 ---
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# --- 啟動 ---
if __name__ == "__main__":
    try:
        loop.create_task(listen_forever())
        loop.run_forever()
    except KeyboardInterrupt:
        print("[INFO] KeyboardInterrupt caught")

