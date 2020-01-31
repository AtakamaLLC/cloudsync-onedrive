"""Imported test suite"""

import io
from cloudsync.tests import *


test_report_info = None


def test_report_info_od(provider):
    temp_name = provider.temp_name()
    provider.get_quota()["used"]
    provider.create(temp_name, io.BytesIO(b"test" * 1000))
    pinfo2 = provider.get_quota()
    assert pinfo2['used'] > 0
    assert pinfo2['limit'] > 0
