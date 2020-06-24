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

def test_url_encoding(provider):
    expected_paths = []

    # Files with a pound causes OneDrive to throw an Error if not url encoded
    dest = provider.temp_name("de'st ##.txt")
    rename_dest = provider.temp_name("rename # dest '' ##.txt")
    fold = provider.temp_name("fo'lder #'#")
    sub_fold = fold + "/sub f'o'lder ##"
    dest_empty = fold + "/dest empty '##'.txt"
    sub_dest = sub_fold + "/sub 'dest' empty ##.txt"
    sub_dest_rename = sub_fold + "/sub rename 'dest' empty ##.txt"

    provider.mkdir(fold)
    expected_paths.append(fold)
    provider.mkdir(sub_fold)
    expected_paths.append(sub_fold)

    info = provider.create(dest, io.BytesIO(b"hello"))
    provider.rename(info.oid, rename_dest)
    expected_paths.append(rename_dest)
    assert provider.exists_path(rename_dest) #nosec
    provider.download(info.oid, io.BytesIO())

    # Hits different endpoint if file is zero bytes
    empty_info = provider.create(dest_empty, io.BytesIO())
    provider.delete(empty_info.oid)
    sub_info = provider.create(sub_dest, io.BytesIO(b"chow"))
    provider.rename(sub_info.oid, sub_dest_rename)
    expected_paths.append(sub_dest_rename)

    fold_info = provider.info_path(fold)
    provider.listdir(fold_info.oid)
    sub_fold_info = provider.info_path(sub_fold)
    provider.listdir(sub_fold_info.oid)

    ents = list(provider.walk("/"))
    assert len(ents) == len(expected_paths) #nosec
    for ent in ents:
        assert ent.path in expected_paths #nosec

def test_two_step_rename(provider):
    parent1 = provider.temp_name("base1")
    parent2 = provider.temp_name("base2")
    provider.mkdir(parent1)
    provider.mkdir(parent2)

    base1 = "case"
    base2 = "CASE"
    path = parent1 + "/" + base1
    # Don't do 2 step rename, parent paths don't match
    one_step = parent2 + "/" + base2
    # Do 2 step rename, paths match except case of base
    two_step = parent1 + "/" + base2

    def not_so_random(length):
        return b'a' * length

    file_info = provider.create(path, io.BytesIO(b"hello"))
    # Kind of hacky but a call to os.urandom indicates we are doing a 2 step rename
    with patch("os.urandom", side_effect=not_so_random) as m:
        provider.rename(file_info.oid, one_step)
        m.assert_not_called()

    file_info = provider.create(path, io.BytesIO(b"hello"))
    with patch("os.urandom", side_effect=not_so_random) as m:
        provider.rename(file_info.oid, two_step)
        m.assert_called()

