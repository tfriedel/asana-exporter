import threading

from loguru import logger as LOG

LOCK = threading.Lock()


def with_lock(f):
    def with_lock_inner(*args, **kwargs):
        with LOCK:
            return f(*args, **kwargs)

    return with_lock_inner


def required(opts):
    """
    @param opts: dict where key is attr name and val is opt name.
    """

    def _required(f):
        def _inner_required(self, *args, **kwargs):
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
