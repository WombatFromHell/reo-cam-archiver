"""
Graceful exit handling for the Camera Archiver application.
"""

import signal
import sys
import threading

from .utils import OUTPUT_LOCK


class GracefulExit:
    """Thread-safe flag for graceful exit handling"""

    def __init__(self):
        self._exit_requested = False
        self._lock = threading.Lock()

    def request_exit(self) -> None:
        with self._lock:
            self._exit_requested = True

    def should_exit(self) -> bool:
        with self._lock:
            return self._exit_requested


def setup_signal_handlers(graceful_exit: GracefulExit) -> None:
    """Setup signal handlers for graceful exit"""

    def signal_handler(signum: int, frame) -> None:
        graceful_exit.request_exit()

        # Convert signal number to signal name
        signal_name = "unknown"
        if signum == signal.SIGINT:
            signal_name = "SIGINT"
        elif signum == signal.SIGTERM:
            signal_name = "SIGTERM"
        elif signum == signal.SIGHUP:
            signal_name = "SIGHUP"
        else:
            signal_name = f"signal {signum}"

        with OUTPUT_LOCK:  # Use global lock to coordinate with progress updates
            sys.stderr.write(f"\nReceived {signal_name}, shutting down gracefully...\n")
            sys.stderr.flush()

    signals = [signal.SIGINT, signal.SIGTERM, signal.SIGHUP]
    for sig in signals:
        try:
            signal.signal(sig, signal_handler)
        except (ValueError, OSError):
            pass
