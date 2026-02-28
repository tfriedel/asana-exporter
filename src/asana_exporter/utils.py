import threading
from collections.abc import Callable
from typing import Any

from loguru import logger as LOG  # noqa: F401 - re-exported

LOCK = threading.Lock()


def with_lock(f: Callable[..., Any]) -> Callable[..., Any]:
    def with_lock_inner(*args: Any, **kwargs: Any) -> Any:
        with LOCK:
            return f(*args, **kwargs)

    return with_lock_inner


def required(opts: dict[str, str]) -> Callable[..., Any]:
    """@param opts: dict where key is attr name and val is opt name."""

    def _required(f: Callable[..., Any]) -> Callable[..., Any]:
        def _inner_required(self: Any, *args: Any, **kwargs: Any) -> Any:
            has = all([hasattr(self, o) for o in opts])
            if not has or not all([getattr(self, o) for o in opts]):
                msg = (
                    "one or more of the following required options have "
                    f"not been provided: {', '.join(opts.values())}"
                )
                raise Exception(msg)
            return f(self, *args, **kwargs)

        return _inner_required

    return _required
