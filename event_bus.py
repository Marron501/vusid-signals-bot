"""
Lightweight in-process pub/sub for SSE (Server-Sent Events).
Import and call publish() from anywhere; the dashboard SSE endpoint subscribes.
"""
from __future__ import annotations
import threading
from queue import Empty, Queue

_lock        = threading.Lock()
_subscribers: list[Queue] = []


def subscribe() -> Queue:
    """Register a new SSE subscriber. Returns a queue to read events from."""
    q: Queue = Queue(maxsize=200)
    with _lock:
        _subscribers.append(q)
    return q


def unsubscribe(q: Queue) -> None:
    with _lock:
        try:
            _subscribers.remove(q)
        except ValueError:
            pass


def publish(event: dict) -> None:
    """Broadcast event dict to every active subscriber (non-blocking)."""
    with _lock:
        dead = []
        for q in _subscribers:
            try:
                q.put_nowait(event)
            except Exception:
                dead.append(q)
        for q in dead:
            _subscribers.remove(q)
