"""

The run_in_thread function is used to automate and sanity
check running tests in this module
"""

from typing import Callable
from threading import Thread

# helper function which runs a function in a separate thread
# so that we have no possibility of re-using connections
#
# we could set check_same_thread on sqlite_with_wal to be False,
# but often applications won't give us that luxury, so we should
# test with 'default' parameters
def run_in_thread(func: Callable[[], None], *, allow_unwrapped: bool = False) -> None:
    assert callable(func), "Didn't pass a function to run_in_thread"
    if not allow_unwrapped:
        # https://github.com/bjoluc/pytest-reraise/blob/main/pytest_reraise/reraise.py#L110
        # make sure this is a wrapped func
        assert (
            func.__code__.co_name == "wrapped"
        ), "Didn't match wrapped function name -- try to wrap this in @reraise.wrap or pass allow_unwrapped if trying to catch an error"

    # run in a separate thread
    t = Thread(target=func)
    t.start()
    t.join()
