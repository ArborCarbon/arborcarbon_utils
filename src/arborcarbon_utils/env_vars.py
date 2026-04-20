"""Base class for environment variable accessors used across ArborCarbon services."""

import logging
import os
from functools import cached_property
from pathlib import Path

from arborcarbon_utils.common import truthy


##################################################################################################
# Classes
##################################################################################################
class BaseEnvVars:
    """
    Cached view of process environment variables with common ArborCarbon defaults.

    Subclass this to add service-specific variables. The ``@singleton`` decorator
    ensures a single shared instance is returned regardless of how many times the
    subclass is constructed.

    Example::

        from arborcarbon_utils.env_vars import BaseEnvVars
        from arborcarbon_utils.common import singleton

        @singleton
        class MyEnvVars(BaseEnvVars):
            @cached_property
            def my_setting(self) -> str:
                return self.env_var_get("MY_SETTING", "default")

        g = MyEnvVars()
    """

    ##############################################################################################
    # Constants
    ##############################################################################################
    FALSE = "false"

    ##############################################################################################
    # Common environment variables
    ##############################################################################################
    @cached_property
    def container_path(self) -> str:
        """Filesystem root used by services when running inside their container."""
        return self.env_var_get("CONTAINER_PATH", "/app")

    @cached_property
    def db_url(self) -> str:
        """PostgreSQL connection URL."""
        return self.env_var_get("DATABASE_URL")

    @cached_property
    def in_container(self) -> bool:
        """Whether the current process is running inside a container."""
        return truthy(self.env_var_get("IN_CONTAINER", self.FALSE))

    @cached_property
    def is_dev(self) -> bool:
        """Whether the current runtime should use development-friendly defaults."""
        return truthy(self.env_var_get("IS_DEV", self.FALSE))

    @cached_property
    def log_file(self) -> str:
        """Relative path to the shared log file when file logging is enabled."""
        return self.env_var_get("LOG_FILE", "data/debug.log")

    ##############################################################################################
    # Public methods
    ##############################################################################################
    @staticmethod
    def env_var_get(env: str, default_var: str | None = None) -> str:
        """Return an environment variable value, or the supplied default when it is unset."""
        env_var = os.environ.get(env)
        output = default_var if env_var is None else env_var
        if output is None:
            error = f"No {env} value in environment variables"
            raise ValueError(error)
        return output

    def setup_logging(self):
        """
        Configure root-level logging; safe to call multiple times (basicConfig is idempotent).

        Logs go to a file when running inside a container (``IN_CONTAINER=true``),
        and to stdout otherwise.
        """
        filename = Path(self.container_path) / Path(self.log_file)
        logging.basicConfig(
            filename=str(filename) if self.in_container else None,
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
        )

    ##############################################################################################
    # Private methods
    ##############################################################################################
    def _clear_cache(self):
        """
        Clear all cached env-var values — **for testing only**.

        Forces re-evaluation on the next access so tests can monkeypatch
        ``os.environ``, call code under test, then restore a clean state.
        """
        self.__dict__.clear()
