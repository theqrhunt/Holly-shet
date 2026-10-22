import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from collections import deque
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException
from TikTokLive import TikTokLiveClient
from TikTokLive.events import (
    CommentEvent,
    GiftEvent,
    ConnectEvent,
    DisconnectEvent,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tiktok-api")

# Render environment variables
TIKTOK_USERNAME = "nostalagiaai"
API_KEY = "api19379Hao-@29Uwi7283772G"
MAX_EVENTS = 5000

# Temporary in-memory event queue
events_queue = deque()
queue_lock = asyncio.Lock()
next_event_id = 1

state = {
    "connected": False,
    "last_error": None,
    "last_connected_at": None,
    "last_event_at": None,
}

stop_event = None


def get_timestamp():
    """Return the current Unix timestamp and readable UTC time."""
    timestamp = time.time()
    received_at = datetime.fromtimestamp(
        timestamp, tz=timezone.utc
    ).isoformat()

    return timestamp, received_at


async def add_event(data: dict):
    """Add an event to the queue in arrival order."""
    global next_event_id

    async with queue_lock:
        timestamp, received_at = get_timestamp()

        data["id"] = next_event_id
        next_event_id += 1

        # Timestamp recorded when the event is received by this API
        data["timestamp"] = timestamp
        data["received_at"] = received_at

        events_queue.append(data)

        # Prevent unlimited memory usage
        while len(events_queue) > MAX_EVENTS:
            events_queue.popleft()

        state["last_event_at"] = timestamp


async def tiktok_worker():
    """Connect to TikTok LIVE and retry after failures."""
    if not TIKTOK_USERNAME:
        state["last_error"] = "TIKTOK_USERNAME is not configured"
        logger.error(state["last_error"])
        return

    while stop_event and not stop_event.is_set():
        client = TikTokLiveClient(
            unique_id=TIKTOK_USERNAME
        )

        @client.on(ConnectEvent)
        async def on_connect(event: ConnectEvent):
            state["connected"] = True
            state["last_error"] = None
            state["last_connected_at"] = time.time()
            logger.info("Connected to @%s", TIKTOK_USERNAME)

        @client.on(DisconnectEvent)
        async def on_disconnect(event: DisconnectEvent):
            state["connected"] = False
            logger.warning("Disconnected from TikTok LIVE")

        @client.on(CommentEvent)
        async def on_comment(event: CommentEvent):
            await add_event({
                "type": "comment",
                "username": (
                    getattr(event.user, "nickname", None)
                    or getattr(event.user, "unique_id", "Unknown")
                ),
                "text": event.comment,
            })

        @client.on(GiftEvent)
        async def on_gift(event: GiftEvent):
            gift = event.gift

            await add_event({
                "type": "gift",
                "username": (
                    getattr(event.user, "nickname", None)
                    or getattr(event.user, "unique_id", "Unknown")
                ),
                "gift": getattr(gift, "name", None) or "Unknown gift",
                "repeat_count": getattr(event, "repeat_count", 1),
            })

        try:
            logger.info("Connecting to @%s...", TIKTOK_USERNAME)

            # TikTokLive's start() starts its connection loop.
            await client.start()

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            state["last_error"] = str(exc)
            logger.exception("TikTok connection failed")

        finally:
            state["connected"] = False

            try:
                if getattr(client, "connected", False):
                    await client.disconnect()
            except Exception:
                logger.exception("Error closing TikTok connection")

        if not stop_event or stop_event.is_set():
            break

        logger.info("Retrying TikTok connection in 5 seconds...")

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=5,
            )
        except asyncio.TimeoutError:
            pass


async def check_api_key(
    x_api_key: str | None = Header(default=None),
):
    # API_KEY is optional. Configure it in Render to protect /events.
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key",
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global stop_event

    stop_event = asyncio.Event()
    worker = asyncio.create_task(tiktok_worker())

    try:
        yield
    finally:
        stop_event.set()
        worker.cancel()

        try:
            await worker
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="TikTok LIVE Events API",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    return {
        "service": "TikTok LIVE Events API",
        "endpoints": ["/health", "/events"],
    }


@app.get("/health")
async def health():
    async with queue_lock:
        queue_size = len(events_queue)

    return {
        "ok": True,
        "tiktok_username": TIKTOK_USERNAME or None,
        "connected": state["connected"],
        "queued_events": queue_size,
        "last_error": state["last_error"],
        "last_connected_at": state["last_connected_at"],
        "last_event_at": state["last_event_at"],
    }


@app.get("/events", dependencies=[Depends(check_api_key)])
async def get_events():
    """Return all queued events in order, then clear the queue."""
    async with queue_lock:
        batch = list(events_queue)
        events_queue.clear()

    return {
        "count": len(batch),
        "events": batch,
    }
