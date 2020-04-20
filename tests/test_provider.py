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
    # file_upload_size = 4 MiB, should take 12/4=3 API calls to upload file
    file_size = 12 * 1024 * 1024
    data = BytesIO(os.urandom(file_size))
    dest = provider.temp_name("dest")

    def hit_api(action, path=None, url=None, data=None, headers=None, json=None):
        if not url:
            url = provider._get_url(path)

        with provider._api() as client:
            if not url:
                path = path.lstrip("/")
                url = client.base_url + path
            head = {
                      'Authorization': 'bearer {access_token}'.format(access_token=client.auth_provider.access_token),
                      'content-type': 'application/json'}
            if headers:
                head.update(headers)
            for k in head:
                head[k] = str(head[k])
            log.debug("hit_api %s %s", action, url)
            req = getattr(requests, action)(
                url,
                stream=None,
                headers=head,
                json=json,
                data=data)

        if req.status_code > 202:
            raise Exception("Unknown error %s %s" % (req.status_code, req.json()))

        res = req.json()
        return res

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
                return hit_api(action, path, url, data, headers, json)

        # Send all other api calls through to onedrive
        else:
            return hit_api(action, path, url, data, headers, json)

    with patch.object(provider.prov, "_direct_api", side_effect=flaky_api):
        provider.create(dest, data)

    root_info = provider.info_path("/")
    dir_list = list(provider.listdir(root_info.oid))
    log.debug("dir_list=%s", dir_list)
    assert len(dir_list) == 1
    assert dir_list[0].size == file_size

