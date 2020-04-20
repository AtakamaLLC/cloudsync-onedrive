"""Imported test suite"""

import io
import requests
from cloudsync.tests import *
from unittest.mock import patch

test_report_info = None


def test_report_info_od(provider):
    temp_name = provider.temp_name()
    provider.get_quota()["used"]
    provider.create(temp_name, io.BytesIO(b"test" * 1000))
    pinfo2 = provider.get_quota()
    assert pinfo2['used'] > 0
    assert pinfo2['limit'] > 0

def test_interrupted_file_upload(provider):
    # Should take 3 successful API calls to upload file
    provider.prov.upload_block_size = 320 * 1024
    file_size = 3 * provider.prov.upload_block_size
    data = BytesIO(os.urandom(file_size))
    dest = provider.temp_name("dest")
    
    direct_api = provider.prov._direct_api

    # Every other attempt throws a DisconnectError
    api_upload_calls = 0
    def flaky_api(action, path=None, url=None, data=None, headers=None, json=None):
        nonlocal api_upload_calls

        # Temporary url is different every time, use Content-Range in header to identify upload call 
        if headers and "Content-Range" in headers:
            api_upload_calls += 1
            if api_upload_calls % 2:
                raise CloudDisconnectedError("Not connected")
            else:
                return direct_api(action, path=path, url=url, data=data, headers=headers, json=json)

        # Send all other api calls through to onedrive
        else:
            return direct_api(action, path=path, url=url, data=data, headers=headers, json=json)

    with patch.object(provider.prov, "_direct_api", side_effect=flaky_api):
        info = provider.create(dest, data)

    new_fh = BytesIO()
    provider.download(info.oid, new_fh)
    new_fh.seek(0, SEEK_END)
    new_len = new_fh.tell()
    assert new_len == file_size #nosec

