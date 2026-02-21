"""Queue manager for decoupling fetcher and processor.

Uses asyncio.Queue to buffer fetched data before processing.
This replaces the Celery-based architecture.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Any

logger = logging.getLogger(__name__)


@dataclass
class OCDataItem:
    """Represents a single option chain data item to be processed."""
    instrument: dict
    expiry: str
    oc_response: dict
    closing_snapshot_time: Optional[Any] = None


class OCQueue:
    """Manages the queue between fetcher and processor.
    
    This is a simple wrapper around asyncio.Queue that provides
    type safety and logging for option chain data.
    """
    
    def __init__(self, maxsize: int = 100):
        """Initialize the queue.
        
        Args:
            maxsize: Maximum number of items in queue. 0 = unlimited.
                    100 provides backpressure if processing is slow.
        """
        self._queue: asyncio.Queue[OCDataItem] = asyncio.Queue(maxsize=maxsize)
        self._items_processed = 0
        self._items_queued = 0
        logger.info(f"[QUEUE] Initialized with maxsize={maxsize}")
    
    async def put(self, item: OCDataItem) -> None:
        """Add an item to the queue.
        
        If queue is full, this will wait until space is available.
        """
        await self._queue.put(item)
        self._items_queued += 1
        logger.debug(f"[QUEUE] Item added. Queue size: {self._queue.qsize()}")
    
    async def get(self) -> OCDataItem:
        """Get an item from the queue.
        
        If queue is empty, this will wait until an item is available.
        """
        item = await self._queue.get()
        self._items_processed += 1
        return item
    
    def task_done(self) -> None:
        """Mark the last get() operation as complete.
        
        Should be called after processing the item.
        """
        self._queue.task_done()
    
    def qsize(self) -> int:
        """Return the current size of the queue."""
        return self._queue.qsize()
    
    def empty(self) -> bool:
        """Return True if queue is empty."""
        return self._queue.empty()
    
    def get_stats(self) -> dict:
        """Return queue statistics."""
        return {
            "queued": self._items_queued,
            "processed": self._items_processed,
            "pending": self._items_queued - self._items_processed,
            "current_size": self.qsize(),
        }


# Global queue instance - will be initialized in main.py
oc_queue: Optional[OCQueue] = None


def init_queue(maxsize: int = 100) -> OCQueue:
    """Initialize the global queue instance."""
    global oc_queue
    oc_queue = OCQueue(maxsize=maxsize)
    return oc_queue


def get_queue() -> OCQueue:
    """Get the global queue instance.
    
    Raises:
        RuntimeError: If queue hasn't been initialized.
    """
    if oc_queue is None:
        raise RuntimeError("Queue not initialized. Call init_queue() first.")
    return oc_queue
