"""Executor whose abandoned workers cannot extend interpreter shutdown."""

from __future__ import annotations

import queue
import threading
from concurrent.futures import Executor, Future
from typing import Callable, TypeVar


_Result = TypeVar("_Result")
_STOP = object()


class DaemonThreadPoolExecutor(Executor):
    """A small fixed thread pool with daemon workers and bounded shutdown.

    Python's standard ``ThreadPoolExecutor`` registers every worker for an
    unbounded interpreter-exit join, including daemonized subclasses.  Runtime
    dependency calls can therefore defeat an otherwise bounded shutdown.  This
    executor deliberately does not register an exit hook; callers still drain
    normally, while an irrecoverably blocked dependency cannot hold the process
    open after the configured shutdown deadline.
    """

    def __init__(self, max_workers: int, *, thread_name_prefix: str = "worker"):
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")
        self._tasks: queue.Queue[object] = queue.Queue()
        self._lock = threading.Lock()
        self._shutdown = False
        self._threads = tuple(
            threading.Thread(
                target=self._worker,
                name=f"{thread_name_prefix}_{index}",
                daemon=True,
            )
            for index in range(max_workers)
        )
        for thread in self._threads:
            thread.start()

    @property
    def max_workers(self) -> int:
        """Return the configured worker-capacity ceiling."""
        return len(self._threads)

    def submit(self, fn: Callable[..., _Result], /, *args, **kwargs) -> Future:
        future = Future()
        with self._lock:
            if self._shutdown:
                raise RuntimeError("cannot schedule new futures after shutdown")
            self._tasks.put((future, fn, args, kwargs))
        return future

    def _worker(self) -> None:
        while True:
            task = self._tasks.get()
            try:
                if task is _STOP:
                    return
                future, fn, args, kwargs = task
                if future.set_running_or_notify_cancel():
                    try:
                        future.set_result(fn(*args, **kwargs))
                    except BaseException as exc:
                        future.set_exception(exc)
            finally:
                self._tasks.task_done()

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        with self._lock:
            if not self._shutdown:
                self._shutdown = True
                if cancel_futures:
                    self._cancel_queued_futures()
                for _ in self._threads:
                    self._tasks.put(_STOP)
        if wait:
            for thread in self._threads:
                if thread is not threading.current_thread():
                    thread.join()

    def _cancel_queued_futures(self) -> None:
        retained = []
        while True:
            try:
                task = self._tasks.get_nowait()
            except queue.Empty:
                break
            try:
                if task is _STOP:
                    retained.append(task)
                else:
                    task[0].cancel()
            finally:
                self._tasks.task_done()
        for task in retained:
            self._tasks.put(task)
