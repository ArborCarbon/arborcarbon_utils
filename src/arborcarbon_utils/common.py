"""Small shared coercion and decorator helpers used across ArborCarbon services."""

from functools import wraps
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable

##################################################################################################
# Global Constants
##################################################################################################
TClass = TypeVar("TClass")


##################################################################################################
# Global Methods
##################################################################################################
def singleton[TClass](cls: Callable[..., TClass]) -> Callable[..., TClass]:
    """Decorate a class so repeated construction returns a single shared instance."""
    instance: TClass | None = None

    @wraps(cls)
    def wrapper_singleton(*args: tuple, **kwargs: dict[str, Any]) -> TClass:
        nonlocal instance
        if instance is None:
            instance = cls(*args, **kwargs)
        return instance

    return wrapper_singleton


def to_bool(v: object) -> bool:
    """Coerce a supported value into a boolean using common env/config truthy strings."""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    error = f"not a bool: {v!r}"
    raise TypeError(error)


def to_float(v: object) -> float:
    """Coerce a numeric or numeric-string value into a float."""
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        return float(v.strip())
    error = f"not a float: {v!r}"
    raise TypeError(error)


def to_int(v: object) -> int:
    """Coerce a numeric or numeric-string value into an integer."""
    if isinstance(v, bool):
        error = "bool is not int for config"
        raise TypeError(error)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str):
        return int(v.strip())
    error = f"not an int: {v!r}"
    raise TypeError(error)


def to_str_upper(v: object) -> str:
    """Coerce a value to a stripped uppercase string."""
    if not isinstance(v, str):
        v = str(v)
    return v.strip().upper()


def truthy(v: str | None) -> bool:
    """Interpret an optional string using the shared truthy token set."""
    return v.strip().lower() in {"1", "true", "t", "yes", "y", "on"} if v else False
