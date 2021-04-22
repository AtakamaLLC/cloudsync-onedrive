"""Imported test suite"""

import cloudsync_onedrive
from cloudsync.tests import *
from unittest.mock import patch, Mock


def test_report_info_od(provider):
    temp_name = provider.temp_name()
    before = provider.get_quota()["used"]
    provider.create(temp_name, io.BytesIO(b"test" * 100000))
    with patch.object(provider.prov._personal_drive.drives[0], "owner", "personal-drive-owner"):
        pinfo2 = provider.get_quota()
        assert pinfo2['used'] > before
        assert pinfo2['limit'] > 0
        assert pinfo2['login'] == "personal-drive-owner"


def test_globalize_oid(provider):
    goid = provider.globalize_oid("root")
    assert provider.info_oid(goid).path == "/"
    assert goid != "root"

    already_global_oid = provider.mkdir("root")
    assert provider.globalize_oid(already_global_oid) == already_global_oid


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


def test_event_filter_walk(provider):
    # mock out sync_state interface
    class MockState:
        def get_path(self, oid):
            return None
    provider.prov.sync_state = MockState()

    # set a sync root and drain events
    root_oid = provider.mkdir("root")
    root = provider.set_root(root_oid=root_oid)
    assert root_oid == root[1]
    for _ in provider.events():
        pass

    # create a folder+file outside of root, ensure no events are generated (these events should be filtered out)
    irrelevant_folder_oid = provider.mkdir("irrelevant")
    irrelevant_file_oid = provider.create("irrelevant/whatever.txt", io.BytesIO(b"whatever")).oid
    for e in provider.events():
        if e.oid == irrelevant_file_oid or e.oid == irrelevant_folder_oid:
            assert False

    # move said folder+file into root, ensure we get an event for each
    provider.rename(irrelevant_folder_oid, "root/relevant")
    got_folder_event = False
    got_file_event = False
    for e in provider.events():
        if e.oid == irrelevant_folder_oid:
            got_folder_event = True
        elif e.oid == irrelevant_file_oid:
            got_file_event = True
    assert got_folder_event
    assert got_file_event


shared_folder_test_namespaces = {
    # to get a namespace ID: use the provider.list_ns() function, either via the CLI or a test, to list all
    # available namespaces for a given account/credentials
    "onedrive": Namespace("shared", os.environ.get("ONEDRIVE_SHARED_NS_ID")),
    "testodbiz": Namespace("shared", os.environ.get("TESTODBIZ_SHARED_NS_ID")),
}


@pytest.fixture(name="shared_folder_prov")
def shared_folder_prov_fixture(config_provider):
    ns = shared_folder_test_namespaces.get(config_provider.name)
    if ns:
        with patch("cloudsync_onedrive.OneDriveProvider._test_namespace") as mock_test_namespace:
            mock_test_namespace.__get__ = Mock(return_value=ns)
            yield from mixin_provider(config_provider)


def test_shared_folder_basic(shared_folder_prov):
    prov = shared_folder_prov
    log.info("NS=%s", prov.namespace)
    ns_id = prov.namespace_id
    prov.disconnect()
    prov.reconnect()
    assert prov.namespace_id == ns_id

    # get_quota should return something - however, in most cases, used/remaining are 0
    # (user has limited permissions, unable to stat the drive of another user)
    assert prov.get_quota()

    # prime events / cursor
    for _ in prov.events():
        pass

    # create some dirs and files
    r = prov.mkdir("/root")
    s2 = prov.mkdirs("/root/sub1/sub2")
    s1 = prov.info_path("/root/sub1").oid
    assert r and s1 and s2
    assert prov.exists_path("/root/sub1")
    assert prov.info_oid(s2).path == "/root/sub1/sub2"
    f1 = prov.create("/file1", BytesIO(b"file1")).oid
    f2 = prov.create("/root/file2", BytesIO(b"file2")).oid
    f3 = prov.create("/root/sub1/sub2/file3", BytesIO(b"file3")).oid
    assert f1 and f2 and f3
    assert prov.exists_oid(f3)
    assert prov.info_path("/root/file2").oid == f2

    # upload/download
    up_small = prov.upload(f1, BytesIO(b""))
    assert up_small.size == 0
    down_small = BytesIO()
    prov.download(f1, down_small)
    assert len(down_small.getvalue()) == 0
    up_large = prov.upload(f1, BytesIO(b"many-bytes" * 1000))
    assert up_large.size == 10 * 1000
    down_large = BytesIO()
    prov.download(f1, down_large)
    assert len(down_large.getvalue()) == 10 * 1000

    # set sync root to a sub-folder of the namespace's root
    root = prov.set_root("/root", r)
    assert root == ("/root", r)

    # test event filtering
    event_oids = set()
    for e in prov.events():
        event_oids.add(e.oid)
    # file1 is outside the sync root, expect its events to be filtered out
    assert event_oids == {r, s1, s2, f2, f3}

    # test walks
    walk_oids = set()
    for e in prov.walk("/"):
        walk_oids.add(e.oid)
    assert walk_oids == {r, s1, s2, f1, f2, f3}
    walk_oids.clear()
    for e in prov.walk_oid(s1):
        walk_oids.add(e.oid)
    assert walk_oids == {s2, f3}

    # filesystem operations
    prov.delete(f3)
    prov.delete(s2)
    prov.rename(s1, "/root/sub1-renamed")
    ls_oids = set()
    for ls in prov.listdir(r):
        ls_oids.add(ls.oid)
        if ls.oid == s1:
            assert ls.path == "/root/sub1-renamed"
    assert ls_oids == {s1, f2}
    # dir should be empty after delete of s2 and f3
    assert not list(prov.listdir(s1))

    # backwards compatibility for legacy shared folder namespaces (ODB only)
    legacy_ns_id = ns_id[0:ns_id.rfind("|")]
    if prov._is_biz:
        expected_ns_name = prov.namespace.name[0:prov.namespace.name.rfind("/")]
        prov.namespace_id = legacy_ns_id
        assert prov.namespace.name == expected_ns_name
        assert not prov.namespace.is_shared
    else:
        with pytest.raises(CloudNamespaceError):
            prov.namespace_id = legacy_ns_id
        # put it back to make test teardown happy
        prov.namespace_id = ns_id


def test_shared_folder_odb():
    # specific to OneDrive Business
    # uses "unwrapped" provider instances because the test mixin's test isolation feature gets in the way here

    # provider connected to root of shared folder
    prov1 = cloudsync_onedrive.OneDriveBusinessTestProvider.test_instance()
    prov1.namespace_id = shared_folder_test_namespaces["testodbiz"].id
    prov1.connect(cloudsync_onedrive.OneDriveBusinessTestProvider._test_creds)  # type: ignore
    prov1.set_root(root_path="/")

    # prime events
    _ = prov1.current_cursor

    # provider connected to same drive as prov1, but NOT to the same shared folder namespace
    prov2 = cloudsync_onedrive.OneDriveBusinessTestProvider.test_instance()
    prov2.namespace_id = prov1.namespace.drive_id
    prov2.connect(cloudsync_onedrive.OneDriveBusinessTestProvider._test_creds)  # type: ignore

    # ensure that an irrelevant event generated by a prov2 file creation is ignored by prov1
    f1_info = prov2.create(f"/zzz-cloudsync-tests/irrelevant/{os.urandom(32).hex()}", BytesIO(b"some file"))
    for e in prov1.events():
        assert e.oid != f1_info.oid

    # clean up
    prov2.delete(f1_info.oid)


def test_shared_folder_owner_created_subfolder(shared_folder_prov):
    # precondition:
    #   shared folder contains a sub-folder named "owner-created" that was created by the owner (sharer)
    #   of the shared folder

    # use the raw provider, not the mixin instance (which provides isolation by creating a remote folder per test) --
    # that is NOT what we want here
    prov = shared_folder_prov.prov
    f1_content = BytesIO(b"file1")
    f1_path = f"/owner-created/{os.urandom(32).hex()}"
    f1_oid = prov.create(f1_path, f1_content).oid
    f1_downloaded = BytesIO()
    prov.download(f1_oid, f1_downloaded)
    assert f1_downloaded.read() == f1_content.read()
    prov.delete(f1_oid)
    oids = [item.oid for item in prov.listdir_path("/owner-created")]
    assert f1_oid not in oids
