"""Suite-wide fixtures."""

import logging
from collections.abc import Iterator

import pytest

from limnopulse_api.adapters.telemetry_metrics_log import METRICS_LOGGER


@pytest.fixture(autouse=True)
def _restore_the_metrics_logger() -> Iterator[None]:
    """An ingest-on `create_app` configures this process-global logger (level, its own stream
    handler, `propagate=False`; #116), so restore it: no test inherits another test's app."""
    logger = logging.getLogger(METRICS_LOGGER)
    level, handlers, propagate = logger.level, list(logger.handlers), logger.propagate
    yield
    logger.setLevel(level)
    logger.handlers[:] = handlers
    logger.propagate = propagate
