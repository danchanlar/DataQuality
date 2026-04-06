# dq_engine/tests/conftest.py
import os
import pytest

@pytest.fixture(scope="session")
def run_integration(pytestconfig):
    return bool(pytestconfig.getoption("run_integration"))

@pytest.fixture(scope="session")
def conn_string(pytestconfig, run_integration):
    """
    Build connection string from:
      1) --conn-string CLI option (preferred)
      2) DQ_CONN_STRING environment variable (fallback)
    If neither provided AND tests are marked integration, they will be skipped.
    """
    cs = pytestconfig.getoption("conn_string")
    if not cs:
        cs = os.getenv("DQ_CONN_STRING")

    if not cs and run_integration:
        pytest.skip(
            "Integration tests require --conn-string or DQ_CONN_STRING env var."
        )
    return cs

def pytest_collection_modifyitems(config, items):
    """
    If tests are marked 'integration' but --run-integration is not set,
    skip them automatically.
    """
    if config.getoption("run_integration"):
        return

    skip_it = pytest.mark.skip(reason="use --run-integration to enable integration tests")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_it)