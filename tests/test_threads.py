"""
Me getting used to handling thread exceptions using
https://github.com/bjoluc/pytest-reraise
"""


import pytest
from pytest_reraise import Reraise  # type: ignore[import]

from . import run_in_thread


# a sanity check to make sure that failed assertions
# in wrapped threads raise errors
def test_thread_wrapper_none() -> None:
    def _run_no_wrapper() -> None:
        assert True

    with pytest.raises(AssertionError) as exc_info:
        run_in_thread(_run_no_wrapper)

    assert "Didn't match wrapped function name" in str(exc_info.value)


def test_thread_wrapper_has(reraise: Reraise) -> None:
    @reraise.wrap
    def _run_with_wrapper() -> None:
        assert True

    run_in_thread(_run_with_wrapper)


# https://github.com/bjoluc/pytest-reraise#accessing-and-modifying-exceptions
def test_thread_raises(reraise: Reraise) -> None:
    def _run() -> None:
        with reraise:
            assert False, "Raised error here"

    run_in_thread(_run, allow_unwrapped=True)

    # Return the captured exception
    assert type(reraise.exception) is AssertionError

    # This won't do anything, since an exception has already been captured
    reraise.exception = Exception()

    # Return the exception and set reraise.exception to None
    err = reraise.reset()  # type: ignore
    assert isinstance(err, AssertionError)
    assert "Raised error here" in str(err)

    # Reraise will not fail the test case
    assert reraise.exception is None
