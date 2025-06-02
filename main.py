import asyncio
import signal
import sys

from ws_client import listen_forever, shutdown, websocket

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

# Graceful shutdown
def shutdown_handler(sig, frame):
    print("\n[INFO] Shutting down gracefully...")
    if websocket and websocket.close_code is None:
        loop.create_task(websocket.close())
    loop.create_task(shutdown())

# 註冊 SIGINT 與 SIGTERM
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

if __name__ == "__main__":
    try:
        loop.create_task(listen_forever())
        loop.run_forever()
    except KeyboardInterrupt:
        print("[INFO] KeyboardInterrupt caught")
    finally:
        loop.close()

