import pytest

import cloudsync

cloudsync.logger.setLevel("TRACE")

def pytest_addoption(parser):
    parser.addoption(
        "--manual", action="store_true", default=False, help="run manual tests"
    )

    parser.addoption(
        "--provider", action="append", default=[], help="run provider tests"
    )

def pytest_runtest_setup(item):
    if 'manual' in item.keywords and not item.config.getoption("--manual"):
        pytest.skip("need --manual option to run this test")

