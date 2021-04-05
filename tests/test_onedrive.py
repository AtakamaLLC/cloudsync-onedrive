# pylint: disable=protected-access, missing-docstring, line-too-long # noqa: D100

import io
import json
import logging
import re
import typing
from unittest.mock import patch, call

import pytest

from onedrivesdk_fork.error import ErrorCode
from cloudsync.exceptions import CloudNamespaceError, CloudTokenError, CloudFileNotFoundError, CloudDisconnectedError
from cloudsync.tests.fixtures import FakeApi, fake_oauth_provider
from cloudsync.oauth.apiserver import ApiError, api_route
from cloudsync.provider import Namespace, Event
from cloudsync.sync.state import FILE, DIRECTORY
from cloudsync_onedrive import OneDriveProvider, EventFilter, NamespaceErrors, Site

log = logging.getLogger(__name__)

NEW_TOKEN = "weird-token-od"


class FakeGraphApi(FakeApi):
    multiple_personal_drives = False

    @api_route("/upload")
    def upload(self, ctx, req):
        self.called("upload", (ctx, req))
        return {"@odata.context": "https://graph.microsoft.com/v1.0/$metadata#drives('bdd46067213df13')/items/$entity",
                "@microsoft.graph.downloadUrl": "https://mckvog.bn.files.1drv.com/y4pxeIYeQKLFVu82R-paaa0e99SXlcC2zAz7ipLsi9EKUPVVsjUe-YBY2tXL6Uwr1KX4HP0tvg3kKejnhtmn79J8i6TW0-wYpdNvNCAKxAVi6UiBtIOUVtd75ZelLNsT_MpNzn65PdB5l926mUuPHq4Jqv3_FKdZCr0LmHm_QbbdEFenK3WgvDwFKIZDWCXEAdYxdJPqd2_wk0LVU9ClY4XBIcw84WPA1KdJbABz93ujiA",
                "createdDateTime": "2019-12-04T15:24:18.523Z", "cTag": "aYzpCREQ0NjA2NzIxM0RGMTMhMTAxMi4yNTc",
                "eTag": "aQkRENDYwNjcyMTNERjEzITEwMTIuMQ", "id": "BDD46067213DF13!1012",
                "lastModifiedDateTime": "2019-12-04T15:24:19.717Z", "name": "d943ae092dbf377dd443a9579eb10898.dest",
                "size": 32, "webUrl": "https://1drv.ms/u/s!ABPfE3IGRt0Lh3Q",
                "createdBy": {"application": {"displayName": "Atakama", "id": "4423e6ce"},
                              "user": {"displayName": "Atakama --", "id": "bdd46067213df13"}},
                "lastModifiedBy": {"application": {"displayName": "Atakama", "id": "4423e6ce"},
                                   "user": {"displayName": "Atakama --", "id": "bdd46067213df13"}},
                "parentReference": {"driveId": "bdd46067213df13", "driveType": "personal", "id": "BDD46067213DF13!1011",
                                    "name": "3676c7b907d09b2d9681084a47bcae59",
                                    "path": "/drive/root:/3676c7b907d09b2d9681084a47bcae59"},
                "file": {"mimeType": "application/octet-stream",
                         "hashes": {"quickXorHash": "MO4Q2k+0wIrVLvPvyFNEXjENmJU=",
                                    "sha1Hash": "9B628BE5312D2F5E7B6ADB1D0114BC49595269BE"}},
                "fileSystemInfo": {"createdDateTime": "2019-12-04T15:24:18.523Z",
                                   "lastModifiedDateTime": "2019-12-04T15:24:19.716Z"}}  # noqa

    @api_route("/token")
    def token(self, ctx, req):
        self.called("token", (ctx, req))
        return {
            "token_type": "bearer",
            "refresh_token": NEW_TOKEN,
            "access_token": "a1",
            "expires_in": 340,
            "scope": "yes",
        }

    @api_route("/me/drive")
    def me_drive(self, ctx, req):
        self.called("quota", (ctx, req))
        return {'@odata.context': 'https://graph.microsoft.com/v1.0/$metadata#drives/$entity', 'id': 'bdd46067213df13',
                'driveType': 'personal', 'owner': {'user': {'displayName': 'Atakama --', 'id': 'bdd46067213df13'}},
                'quota': {'deleted': 15735784, 'remaining': 1104878763593, 'state': 'normal', 'total': 1104880336896,
                          'used': 1573303}}

    @api_route("/me/drives")
    def me_drives(self, ctx, req):
        self.called("_fetch_personal_drives", (ctx, req))
        if self.multiple_personal_drives:
            return {'@odata.context': 'https://graph.microsoft.com/v1.0/$metadata#drives',
                    'value': [{'id': 'bdd46067213df13', 'driveType': 'business', 'name': 'personal'},
                              {'id': '31fd31276064ddb', 'driveType': 'business', 'name': 'drive-2'}]}
        return {'@odata.context': 'https://graph.microsoft.com/v1.0/$metadata#drives', 'value': [
            {'id': 'bdd46067213df13', 'driveType': 'business', 'name': 'personal',
             'owner': {'user': {'displayName': 'owner-name', 'id': 'owner-id'}}}]}

    @api_route("/me/drive/sharedWithMe")
    def me_drive_shared_with_me(self, ctx, req):
        self.called("_fetch_shared_drives", (ctx, req))
        # TODO: move this to an external file, read it in from there
        return json.loads("""
        {
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#Collection(driveItem)",
            "value": [ {
                "@odata.type": "#microsoft.graph.driveItem",
                "id": "ITEM_ID_1",
                "name": "from_team",
                "webUrl": "https://test_onedrive.sharepoint.com/sites/site-1/documents/from_team",
                "folder": {"childCount": 0},
                "remoteItem": {
                    "id": "ITEM_ID_1",
                    "name": "from_team",
                    "webUrl": "https://test_onedrive.sharepoint.com/sites/site-1/documents/from_team",
                    "folder": {"childCount": 0},
                    "parentReference": {
                        "driveId": "DRIVE_ID_1",
                        "driveType": "documentLibrary",
                        "id": "ITEM_ID_2"
                    },
                    "shared": {
                        "scope": "users",
                        "sharedDateTime": "2020-05-07T20:39:38Z",
                        "sharedBy": {
                            "user": {
                                "email": "sharer@test.onmicrosoft.com",
                                "displayName": "Stephen Sharer"
                            }
                        }
                    }
                }
            },
            {
                "@odata.type": "#microsoft.graph.driveItem",
                "id": "ITEM_ID_3",
                "name": "from_personal",
                "webUrl": "https://test_onedrive.sharepoint.com/personal/user_co_onmicrosoft_com/Documents/from_personal",
                "folder": {"childCount": 0},
                "remoteItem": {
                    "id": "ITEM_ID_3",
                    "name": "from_personal",
                    "webUrl": "https://test_onedrive.sharepoint.com/personal/user_co_onmicrosoft_com/Documents/from_personal",
                    "folder": {"childCount": 0},
                    "parentReference": {
                        "driveId": "DRIVE_ID_2",
                        "driveType": "business",
                        "id": "ITEM_ID_4"
                    },
                    "shared": {
                        "scope": "users",
                        "sharedDateTime": "2020-05-07T20:39:38Z",
                        "sharedBy": {
                            "user": {
                                "email": "sharer@test.onmicrosoft.com",
                                "displayName": "Stephen Sharer"
                            }
                        }
                    }
                }
            },
            {
                "@odata.type": "#microsoft.graph.driveItem",
                "id": "ITEM_ID_30",
                "name": "inner_folder",
                "webUrl": "https://test_onedrive.sharepoint.com/personal/user2_co_onmicrosoft_com/Documents/from_personal/inner_folder",
                "folder": {"childCount": 0},
                "remoteItem": {
                    "id": "ITEM_ID_30",
                    "name": "from_personal",
                    "webUrl": "https://test_onedrive.sharepoint.com/personal/user2_co_onmicrosoft_com/Documents/from_personal/inner_folder",
                    "folder": {"childCount": 0},
                    "parentReference": {
                        "driveId": "DRIVE_ID_20",
                        "driveType": "business",
                        "id": "ITEM_ID_40"
                    },
                    "shared": {
                        "scope": "users",
                        "sharedDateTime": "2020-05-07T20:39:38Z",
                        "sharedBy": {
                            "user": {
                                "email": "sharer@test.onmicrosoft.com",
                                "displayName": "Stephen Sharer"
                            }
                        }
                    }
                }
            },
            {
                "@odata.type": "#microsoft.graph.driveItem",
                "id": "ITEM_ID_5",
                "name": "some_file",
                "webUrl": "https://test_onedrive.sharepoint.com/personal/user3_co_onmicrosoft_com/Documents/some_file",
                "file": {"mimeType": "application/octet-stream"},
                "remoteItem": {
                    "id": "ITEM_ID_5",
                    "name": "some_file",
                    "webUrl": "https://test_onedrive.sharepoint.com/personal/user3_co_onmicrosoft_com/Documents/some_file",
                    "file": {"mimeType": "application/octet-stream"},
                    "parentReference": {
                        "driveId": "DRIVE_ID_7",
                        "driveType": "business",
                        "id": "ITEM_ID_4"
                    },
                    "shared": {
                        "scope": "users",
                        "sharedDateTime": "2020-05-07T20:39:38Z",
                        "sharedBy": {
                            "user": {
                                "email": "sharer@test.onmicrosoft.com",
                                "displayName": "Stephen Sharer"
                            }
                        }
                    }
                }
            } ]
        }
        """)

    @api_route("/sites/")
    def sites(self, ctx, req):
        self.called("_fetch_sites", (ctx, req))
        # TODO: move this to an external file, read it in from there
        return json.loads("""{
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#sites",
            "value": [
            {
              "createdDateTime": "2019-11-14T23:28:58Z",
              "id": "site-id-1",
              "lastModifiedDateTime": "0001-01-01T08:00:00Z",
              "name": "Community",
              "webUrl": "https://xyz.sharepoint.com/portals/Community",
              "displayName": "Community",
              "root": {},
              "siteCollection": {
                "hostname": "xyz.sharepoint.com"
              }
            },
            {
              "createdDateTime": "2020-04-20T14:11:32Z",
              "description": "OneDrive cloudsync testing",
              "id": "site-id-2",
              "lastModifiedDateTime": "2020-06-11T00:35:07Z",
              "name": "cloudsync-test-1",
              "webUrl": "https://xyz.sharepoint.com/sites/cloudsync-test-1",
              "displayName": "cloudsync-test-1",
              "root": {},
              "siteCollection": {
                "hostname": "xyz.sharepoint.com"
              }
            },
            {
              "createdDateTime": "2020-06-10T22:48:59Z",
              "description": "test sub-sites",
              "id": "xyz.sharepoint.com,ffffffff-7777-ffff-eeee-acccaeeccccc,aaaaaaaa-1111-cccc-eeee-ddddddc00000",
              "lastModifiedDateTime": "2020-06-10T22:49:03Z",
              "name": "sub-1",
              "webUrl": "https://xyz.sharepoint.com/sites/cloudsync-test-1/sub-1"
            } ] }
        """)

    @api_route("/drives/")
    def default(self, ctx, req):
        upload_url = self.uri("/upload")
        meth = ctx.get("REQUEST_METHOD")
        uri = ctx.get("PATH_INFO")

        if meth == "GET":
            self.called("get", (uri,))
            log.debug("getting")

            if uri.startswith("/drives/namespace-not-found/"):
                raise ApiError(400, json={"error": {"code": ErrorCode.ItemNotFound, "message": uri}})

            if uri.startswith("/drives/item-not-found/"):
                raise ApiError(404, json={"error": {"code": ErrorCode.ItemNotFound, "message": uri}})

            if re.match(r"^/drives/[^/]+/$", uri):
                return {'@odata.context': 'https://graph.microsoft.com/v1.0/$metadata#drives/$entity',
                        'id': 'bdd46067213df13', 'name': 'drive-name', 'driveType': 'personal',
                        'owner': {'user': {'displayName': 'Atakama --', 'id': 'bdd46067213df13'}},
                        'quota': {'deleted': 519205504, 'remaining': 1104878758982, 'state': 'normal',
                                  'total': 1104880336896, 'used': 1577914}}

            if uri.find("ITEM_ID_30") > 0:
                return {
                    "id": "ITEM_ID_30",
                    "name": "from_personal",
                    "webUrl": "https://test_onedrive.sharepoint.com/personal/user2_co_onmicrosoft_com/Documents/from_personal/inner_folder",
                    "folder": {"childCount": 0},
                    "parentReference": {
                        "driveId": "DRIVE_ID_20",
                        "driveType": "business",
                        "id": "ITEM_ID_40"
                    },
                    "shared": {
                        "scope": "users",
                        "sharedDateTime": "2020-05-07T20:39:38Z",
                        "sharedBy": {
                            "user": {
                                "email": "sharer@test.onmicrosoft.com",
                                "displayName": "Stephen Sharer"
                            }
                        }
                    }
                }

            err = ApiError(404, json={"error": {"code": ErrorCode.ItemNotFound, "message": "whatever"}})
            log.debug("raising %s", err)
            raise err

        if meth == "POST" and "/createUploadSession" in uri:
            self.called("upload.session", (uri,))
            log.debug("upload")
            return {'@odata.context': 'https://graph.microsoft.com/v1.0/$metadata#microsoft.graph.uploadSession',
                    'expirationDateTime': '2019-12-11T15:32:31.101Z', 'nextExpectedRanges': ['0-'],
                    'uploadUrl': upload_url}

        if meth == "PUT":
            self.called("upload.put", (uri,))
            return {
                "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#drives('bdd46067213df13')/items/$entity",
                "@microsoft.graph.downloadUrl": "https://mckvog.bn.files.1drv.com/y4pxeIYeQKLFVu82R-paaa0e99SXlcC2zAz7ipLsi9EKUPVVsjUe-YBY2tXL6Uwr1KX4HP0tvg3kKejnhtmn79J8i6TW0-wYpdNvNCAKxAVi6UiBtIOUVtd75ZelLNsT_MpNzn65PdB5l926mUuPHq4Jqv3_FKdZCr0LmHm_QbbdEFenK3WgvDwFKIZDWCXEAdYxdJPqd2_wk0LVU9ClY4XBIcw84WPA1KdJbABz93ujiA",
                "createdDateTime": "2019-12-04T15:24:18.523Z", "cTag": "aYzpCREQ0NjA2NzIxM0RGMTMhMTAxMi4yNTc",
                "eTag": "aQkRENDYwNjcyMTNERjEzITEwMTIuMQ", "id": "BDD46067213DF13!1012",
                "lastModifiedDateTime": "2019-12-04T15:24:19.717Z", "name": "d943ae092dbf377dd443a9579eb10898.dest",
                "size": 32, "webUrl": "https://1drv.ms/u/s!ABPfE3IGRt0Lh3Q",
                "createdBy": {"application": {"displayName": "Atakama", "id": "4423e6ce"},
                              "user": {"displayName": "Atakama --", "id": "bdd46067213df13"}},
                "lastModifiedBy": {"application": {"displayName": "Atakama", "id": "4423e6ce"},
                                   "user": {"displayName": "Atakama --", "id": "bdd46067213df13"}},
                "parentReference": {"driveId": "bdd46067213df13", "driveType": "personal", "id": "BDD46067213DF13!1011",
                                    "name": "3676c7b907d09b2d9681084a47bcae59",
                                    "path": "/drive/root:/3676c7b907d09b2d9681084a47bcae59"},
                "file": {"mimeType": "application/octet-stream",
                         "hashes": {"quickXorHash": "MO4Q2k+0wIrVLvPvyFNEXjENmJU=",
                                    "sha1Hash": "9B628BE5312D2F5E7B6ADB1D0114BC49595269BE"}},
                "fileSystemInfo": {"createdDateTime": "2019-12-04T15:24:18.523Z",
                                   "lastModifiedDateTime": "2019-12-04T15:24:19.716Z"}}  # noqa

        if meth == "POST" and "/children" in uri:
            self.called("mkdir", (uri,))
            return {'something': 'here'}

        log.debug("api: %s, %s %s", meth, uri, req)
        return {}


def fake_odp():
    # TODO: shutting this down is slow, fix that and then fix all tests using the api server to shut down, or use fixtures or something
    srv = FakeGraphApi()

    base_url = srv.uri()
    with patch.object(OneDriveProvider, "_base_url", base_url):
        prov = fake_oauth_provider(srv, OneDriveProvider)
        assert srv.calls["token"]
        assert srv.calls["_fetch_personal_drives"]
        # onedrive saves refresh token if creds change
        assert prov._creds["refresh_token"] == NEW_TOKEN
        return srv, prov


def test_latest_cursor():
    _, odp = fake_odp()
    with patch.object(odp, "events") as events:
        with patch.object(odp, "_direct_api") as direct:
            odp.latest_cursor
            events.assert_not_called()
            direct.assert_called_once()


def test_upload():
    srv, odp = fake_odp()
    odp.create("/small", io.BytesIO())
    assert srv.calls["upload.put"]
    odp.create("/big", io.BytesIO(b'12345678901234567890'))
    assert srv.calls["upload.session"]
    assert srv.calls["upload"]


def test_mkdir():
    srv, odp = fake_odp()
    log.info("calls %s", list(srv.calls.keys()))
    odp.mkdir("/dir")
    assert srv.calls["mkdir"]


def test_root_event():
    srv, odp = fake_odp()
    root_event = {'@odata.type': '#microsoft.graph.driveItem', 'createdDateTime': '2019-10-27T05:46:04Z',
                  'id': '01LYWINUF6Y2GOVW7725BZO354PWSELRRZ', 'lastModifiedDateTime': '2020-01-02T15:45:55Z',
                  'name': 'root',
                  'webUrl': 'https://vidaid-my.sharepoint.com/personal/jack_vidaid_onmicrosoft_com/Documents',
                  'size': 6642351, 'parentReference': {'driveId': 'root', 'driveType': 'business'},
                  'fileSystemInfo': {'createdDateTime': '2019-10-27T05:46:04Z',
                                     'lastModifiedDateTime': '2020-01-02T15:45:55Z'}, 'folder': {'childCount': 5},
                  'root': {}}
    non_root_event = {'@odata.type': '#microsoft.graph.driveItem', 'createdDateTime': '2020-01-02T15:44:44Z',
                      'eTag': '"{F0D504AA-C7E0-4B49-B529-63DEB72E09FE},1"', 'id': '01LYWINUFKATK7BYGHJFF3KKLD323S4CP6',
                      'lastModifiedDateTime': '2020-01-02T15:44:44Z', 'name': '20200102-02',
                      'webUrl': 'https://vidaid-my.sharepoint.com/personal/jack_vidaid_onmicrosoft_com/Documents/20200102-02',
                      'cTag': '"c:{F0D504AA-C7E0-4B49-B529-63DEB72E09FE},0"', 'size': 0,
                      'createdBy': {'application': {'displayName': 'Atakama'},
                                    'user': {'email': 'jack@vidaid.onmicrosoft.com', 'displayName': 'jack'}},
                      'parentReference': {'driveId': 'root', 'driveType': 'business',
                                          'id': '01LYWINUF6Y2GOVW7725BZO354PWSELRRZ', 'path': '/drive/root:'},
                      'fileSystemInfo': {'createdDateTime': '2020-01-02T15:44:44Z',
                                         'lastModifiedDateTime': '2020-01-02T15:44:44Z'}, 'folder': {'childCount': 2}}

    assert odp._convert_to_event(root_event, "123") is None
    assert odp._convert_to_event(non_root_event, "123") is not None


def test_event_filter():
    _, odp = fake_odp()

    # root not set
    assert not odp.root_path
    event = Event(FILE, "", "", "", True)
    assert odp._filter_event(event) == EventFilter.PROCESS
    event = Event(DIRECTORY, "", "", "", False)
    assert odp._filter_event(event) == EventFilter.PROCESS
    assert odp._filter_event(None) == EventFilter.IGNORE

    class MockSyncState:
        def get_path(self, oid):
            if oid == "in-root":
                return "/root/path"
            elif oid == "out-root":
                return "/path"
            else:
                return None

    # root set
    odp._root_oid = "root_oid"
    odp._root_path = "/root"
    odp.sync_state = MockSyncState()

    e = Event(FILE, "", "", "", False)
    assert odp._filter_event(e) == EventFilter.IGNORE
    e = Event(FILE, "in-root", "/root/path2", "hash", True)
    assert odp._filter_event(e) == EventFilter.PROCESS
    e = Event(FILE, "in-root", None, None, False)
    assert odp._filter_event(e) == EventFilter.PROCESS
    e = Event(FILE, "out-root", "/path2", "hash", True)
    assert odp._filter_event(e) == EventFilter.IGNORE
    e = Event(FILE, "out-root", None, None, False)
    assert odp._filter_event(e) == EventFilter.IGNORE
    e = Event(FILE, "in-root", "/path2", "hash", True)
    assert odp._filter_event(e) == EventFilter.PROCESS
    e = Event(FILE, "out-root", "/root/path2", "hash", True)
    assert odp._filter_event(e) == EventFilter.PROCESS
    e = Event(DIRECTORY, "out-root", "/root/path2", "hash", True)
    assert odp._filter_event(e) == EventFilter.WALK

    with pytest.raises(ValueError):
        if odp._filter_event(e):
            log.info("this should throw")


def test_namespace_get():
    _, odp = fake_odp()
    ns = odp.namespace
    nsid = odp.namespace_id
    assert ns
    assert nsid
    assert ns.id == nsid
    assert ns.owner == "owner-name"
    assert ns.owner_id == "owner-id"
    assert ns.owner_type == "user"


def test_namespace_set():
    _, odp = fake_odp()

    personal_id = 'bdd46067213df13'
    odp.namespace_id = personal_id
    assert odp.namespace_id == f'personal|{personal_id}'

    shared_id = 'DRIVE_ID_20|ITEM_ID_30'
    odp.namespace_id = f'shared|{shared_id}'
    assert odp.namespace_id == f'shared|{shared_id}'

    site = Namespace(name="site-id-1", id="site-id-1")
    odp.namespace = site
    assert odp.namespace_id == site.id


def test_namespace_multiple_personal_drives():
    srv, odp = fake_odp()
    srv.multiple_personal_drives = True
    odp._fetch_drive_list(clear_cache=True)
    odp.namespace_id = "personal|31fd31276064ddb"
    assert odp.namespace.name == "Personal/drive-2"


def test_namespace_set_err():
    _, odp = fake_odp()
    with pytest.raises(CloudNamespaceError):
        odp.namespace_id = "namespace-not-found"
    with pytest.raises(CloudNamespaceError):
        odp.namespace = Namespace(name="bad-namespace", id="namespace-not-found")
    with pytest.raises(CloudNamespaceError):
        odp.namespace_id = "no-site|no-drive"
    with pytest.raises(CloudNamespaceError):
        odp.namespace_id = "site-id-2|no-drive"


def test_namespace_set_disconn():
    srv, odp = fake_odp()
    odp.disconnect()
    # we do not validate namespaces when disconnected
    odp.namespace = Namespace(name="whatever", id="whatever")
    assert odp.namespace_id == "whatever"
    odp.namespace_id = "namespace-not-found"
    assert odp.namespace.id == "namespace-not-found"
    # but we do validate in connect()
    with patch.object(OneDriveProvider, "_base_url", srv.uri()):
        with pytest.raises(CloudNamespaceError):
            odp.reconnect()


def test_namespace_set_other():
    _, odp = fake_odp()

    def raise_error(_1, _2):
        raise CloudTokenError("yo")

    with patch.object(odp, '_direct_api', side_effect=raise_error):
        with pytest.raises(CloudTokenError):
            odp.namespace = Namespace(name="whatever", id="item-not-found")
        with pytest.raises(CloudTokenError):
            odp.namespace_id = "item-not-found"


def test_list_namespaces():
    api, odp = fake_odp()
    namespace_objs = odp.list_ns(recursive=False)
    assert namespace_objs[0].parent.name == "Personal"
    # ensure there is no recursion in repr (Site has a list of Drives, Drive has a ref to parent Site)
    assert repr(namespace_objs[0]).find("Site") == -1
    namespaces = [ns.name for ns in namespace_objs]
    assert len(namespaces) == 4
    # personal is always there
    assert "Personal" in namespaces
    # shared folders - fake namespace
    assert "Shared With Me" in namespaces
    # sites are listed
    assert "cloudsync-test-1" in namespaces
    # site with missing "displayName" attribute - "name" attribute used instead
    assert "sub-1" in namespaces
    # protals are ignored
    assert "Community" not in namespaces
    # site fetch done once in connect() and again in list_ns()
    assert len(api.calls["_fetch_sites"]) == 1
    # personal has no children
    personal = namespace_objs[0]
    assert not personal.is_parent
    assert not odp.list_ns(parent=personal)
    # shared has 3 children (folders only, shared file is ignored)
    shared = namespace_objs[1]
    assert shared.is_parent
    child_namespaces = odp.list_ns(parent=namespace_objs[1])
    assert len(child_namespaces) == 3

    # recursive
    api2, odp2 = fake_odp()
    namespaces = odp2.list_ns(recursive=True)
    # fetch additional info for 2 sites
    assert len(api2.calls["_fetch_sites"]) == 3

    # parent
    site = Namespace(name="name", id="site-id-1")
    children = odp2.list_ns(parent=site)
    assert not children
    site = Namespace(name="name", id="site-id-2")
    children = odp2.list_ns(parent=site)
    assert children


def test_mtime():
    mtime = OneDriveProvider._parse_time("2020-05-07T20:39:38Z")
    assert mtime == 1588883978
    mtime = OneDriveProvider._parse_time(None)
    assert mtime == 0
    mtime = OneDriveProvider._parse_time("")
    assert mtime == 0
    mtime = OneDriveProvider._parse_time("0")
    assert mtime == 0


def test_walk_filtered_directory():
    api, odp = fake_odp()
    history: typing.Set[str] = set()
    event_file = Event(FILE, "oid7", "", "", True)
    with patch.object(odp, "walk_oid", return_value=[event_file]) as walk:
        for e in odp._walk_filtered_directory("oid1", history):
            assert e.oid == event_file.oid
        for _ in odp._walk_filtered_directory("oid1", history):
            pass
        walk.assert_called_once_with("oid1", recursive=False)

        walk.reset_mock()
        for _ in odp._walk_filtered_directory("oid2", history):
            pass
        walk.assert_called_once_with("oid2", recursive=False)

    event_dir = Event(DIRECTORY, "oid8", "", "", True)
    with patch.object(odp, "walk_oid", return_value=[event_dir]) as walk:
        for e in odp._walk_filtered_directory("oid3", history):
            assert e.oid in [event_dir.oid, "oid3"]
        walk.assert_has_calls([call("oid3", recursive=False), call("oid8", recursive=False)])

        def cloud_fnf_error(oid, recursive=True):
            raise CloudFileNotFoundError(f"{oid}-{recursive}")

        with patch.object(odp, "walk_oid", cloud_fnf_error):
            # should not raise
            for _ in odp._walk_filtered_directory("oid4", history):
                pass


def test_connect_resiliency():
    api, odp = fake_odp()
    odp.disconnect()
    odp._creds = {"access_token": "t", "refresh": "r"}
    direct_api_og = odp._direct_api

    def direct_api_raises_errors(action, path: str):
        if path.find("sites?search=*") > -1:
            raise Exception("no sites for you")
        if path.find("me/drive/sharedWithMe") > -1:
            raise Exception("no sharing")
        return direct_api_og(action, path)

    # ensure non-connectivity errors are ignored
    with patch.object(OneDriveProvider, "_base_url", api.uri()):
        with patch.object(odp, '_direct_api', side_effect=direct_api_raises_errors):
            odp.reconnect()
            # personal namespace is the failsafe
            namespaces = odp.list_ns()
            assert namespaces[0].name == "Personal"
            # namespace errors are saved and can be queried
            errors = odp.list_ns(parent=NamespaceErrors)
            assert len(errors) == 2


def test_connect_raises_token_errors():
    api, odp = fake_odp()
    odp.disconnect()
    odp._creds = {"access_token": "t", "refresh": "r"}
    direct_api_og = odp._direct_api

    def direct_api_raises_errors(action, path: str):
        if path.find("sites?search=*") > -1:
            raise CloudTokenError("bad token")
        if path.find("me/drive/sharedWithMe") > -1:
            raise CloudTokenError("really bad token")
        return direct_api_og(action, path)

    # ensure connectivity errors bubble up
    with patch.object(OneDriveProvider, "_base_url", api.uri()):
        with patch.object(odp, '_direct_api', side_effect=direct_api_raises_errors):
            odp.reconnect()
            with pytest.raises(CloudTokenError):
                odp.list_ns()


def test_connect_exception_handling():
    api, odp = fake_odp()
    error_index = 0

    # bad drive json
    odp._save_drive_info(Site("", ""), {})
    assert odp.list_ns(parent=NamespaceErrors)[error_index].name.find("KeyError('id'") > 0
    error_index += 1

    # bad shared folder json
    odp._save_shared_with_me_info({"remoteItem": {"folder": 0}})
    assert odp.list_ns(parent=NamespaceErrors)[error_index].name.find("KeyError('parentReference'") > 0
    error_index += 1

    # bad site json
    with patch.object(odp, "_direct_api_error_trap", return_value={"value": [{}]}):
        odp._fetch_sites()
        assert odp.list_ns(parent=NamespaceErrors)[error_index].name.find("KeyError('webUrl'") > 0
        error_index += 1

    # missing personal drive
    with patch.object(odp, "_personal_drive", Site("", "")):
        with patch.object(odp, "_direct_api", return_value={"value": []}):
            with pytest.raises(CloudTokenError):
                odp._fetch_personal_drives()

    # malformed namespace id
    with pytest.raises(CloudNamespaceError):
        odp._get_validated_namespace("")


def test_convert_to_event():
    _, odp = fake_odp()

    event_dict: typing.Dict = {
        '@odata.type': '#microsoft.graph.driveItem', 'createdDateTime': '2021-02-02T17:45:01.2948881Z',
        'cTag': 'adDpERDdGMjIyRjQ2QkFDNjQhMTgzNy42Mzc1MjgxODE0MjUyMDAwMDA',
        'eTag': 'aREQ3RjIyMkY0NkJBQzY0ITE4MzcuMTg',
        'id': '31123222F46BAC64!1837', 'lastModifiedDateTime': '2021-03-31T20:09:02.52Z',
        'name': 'SharingTest',
        'webUrl': 'https://1drv.ms/u/s!AABKS6vQU89_bA',
        'lastModifiedBy': {'user': {'displayName': 'Sharee', 'id': '09876bac64'}},
        'parentReference': {'driveId': '78612318bac64', 'driveType': 'personal', 'id': '09183471C64!103'},
        'deleted': {},
        'remoteItem': {'id': '731498DCD3AB4B4A00!108', 'size': 0,
                       'webUrl': 'https://1drv.ms/u/s!AABKJA6vT0AR_bA',
                       'fileSystemInfo': {'createdDateTime': '0001-01-01T08:00:00Z',
                                          'lastModifiedDateTime': '0001-01-01T08:00:00Z'},
                       'folder': {'childCount': 0,
                                  'view': {'viewType': 'thumbnails',
                                           'sortBy': 'name',
                                           'sortOrder': 'ascending'}},
                       'parentReference': {'driveId': '7874bb446b4b4a00', 'driveType': 'personal'},
                       'shared': {'sharedDateTime': '2021-02-02T17:45:01.2948881Z'}}
    }

    # event_dict["deleted"] = {}
    event = odp._convert_to_event(event_dict, "new-cursor")
    assert not event.exists

    event_dict["deleted"]["state"] = "softDeleted"
    event = odp._convert_to_event(event_dict, "new-cursor")
    assert not event.exists

    event_dict["deleted"]["state"] = "hardDeleted"
    event = odp._convert_to_event(event_dict, "new-cursor")
    assert not event.exists

    event_dict["deleted"]["state"] = "deleted"
    event = odp._convert_to_event(event_dict, "new-cursor")
    assert not event.exists

    del event_dict["deleted"]
    # non-delete events require a parent path
    event_dict["parentReference"]["path"] = "/parent/path"
    event = odp._convert_to_event(event_dict, "new-cursor")
    assert event.exists
