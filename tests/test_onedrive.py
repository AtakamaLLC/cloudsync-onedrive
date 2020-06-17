# pylint: disable=protected-access, missing-docstring, line-too-long # noqa: D100

import io
import json
import logging
import re
from unittest.mock import patch

import pytest

from onedrivesdk_fork.error import ErrorCode
from cloudsync.exceptions import CloudNamespaceError, CloudDisconnectedError, CloudTokenError, CloudFileNotFoundError
from cloudsync.tests.fixtures import FakeApi, fake_oauth_provider
from cloudsync.oauth.apiserver import ApiError, api_route
from cloudsync.provider import Namespace
from cloudsync_onedrive import OneDriveProvider

log = logging.getLogger(__name__)

NEW_TOKEN = "weird-token-od"

class FakeGraphApi(FakeApi):
    multiple_personal_drives = False

    @api_route("/upload")
    def upload(self, ctx, req):
        self.called("upload", (ctx, req))
        return {"@odata.context":"https://graph.microsoft.com/v1.0/$metadata#drives('bdd46067213df13')/items/$entity","@microsoft.graph.downloadUrl":"https://mckvog.bn.files.1drv.com/y4pxeIYeQKLFVu82R-paaa0e99SXlcC2zAz7ipLsi9EKUPVVsjUe-YBY2tXL6Uwr1KX4HP0tvg3kKejnhtmn79J8i6TW0-wYpdNvNCAKxAVi6UiBtIOUVtd75ZelLNsT_MpNzn65PdB5l926mUuPHq4Jqv3_FKdZCr0LmHm_QbbdEFenK3WgvDwFKIZDWCXEAdYxdJPqd2_wk0LVU9ClY4XBIcw84WPA1KdJbABz93ujiA","createdDateTime":"2019-12-04T15:24:18.523Z","cTag":"aYzpCREQ0NjA2NzIxM0RGMTMhMTAxMi4yNTc","eTag":"aQkRENDYwNjcyMTNERjEzITEwMTIuMQ","id":"BDD46067213DF13!1012","lastModifiedDateTime":"2019-12-04T15:24:19.717Z","name":"d943ae092dbf377dd443a9579eb10898.dest","size":32,"webUrl":"https://1drv.ms/u/s!ABPfE3IGRt0Lh3Q","createdBy":{"application":{"displayName":"Atakama","id":"4423e6ce"},"user":{"displayName":"Atakama --","id":"bdd46067213df13"}},"lastModifiedBy":{"application":{"displayName":"Atakama","id":"4423e6ce"},"user":{"displayName":"Atakama --","id":"bdd46067213df13"}},"parentReference":{"driveId":"bdd46067213df13","driveType":"personal","id":"BDD46067213DF13!1011","name":"3676c7b907d09b2d9681084a47bcae59","path":"/drive/root:/3676c7b907d09b2d9681084a47bcae59"},"file":{"mimeType":"application/octet-stream","hashes":{"quickXorHash":"MO4Q2k+0wIrVLvPvyFNEXjENmJU=","sha1Hash":"9B628BE5312D2F5E7B6ADB1D0114BC49595269BE"}},"fileSystemInfo":{"createdDateTime":"2019-12-04T15:24:18.523Z","lastModifiedDateTime":"2019-12-04T15:24:19.716Z"}}    # noqa

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
        return {'@odata.context': 'https://graph.microsoft.com/v1.0/$metadata#drives/$entity', 'id': 'bdd46067213df13', 'driveType': 'personal', 'owner': {'user': {'displayName': 'Atakama --', 'id': 'bdd46067213df13'}}, 'quota': {'deleted': 15735784, 'remaining': 1104878763593, 'state': 'normal', 'total': 1104880336896, 'used': 1573303}}

    @api_route("/me/drives")
    def me_drives(self, ctx, req):
        self.called("_fetch_personal_drives", (ctx, req))
        if self.multiple_personal_drives:
            return {'@odata.context': 'https://graph.microsoft.com/v1.0/$metadata#drives', 'value': [{'id': 'bdd46067213df13', 'driveType': 'business', 'name': 'personal'}, {'id': '31fd31276064ddb', 'driveType': 'business', 'name': 'drive-2'}]}
        return {'@odata.context': 'https://graph.microsoft.com/v1.0/$metadata#drives', 'value': [{'id': 'bdd46067213df13', 'driveType': 'business', 'name': 'personal'}]}

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
                    "shared": {"scope": "users"}
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
                    "shared": {"scope": "users"}
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
                    "shared": {"scope": "users"}
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
                    "shared": {"scope": "users"}
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
              "webUrl": "https://xyz.sharepoint.com/sites/cloudsync-test-1/sub-1",
              "displayName": "cloudsync-sub-site-1"
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

            if uri.startswith("/drives/item-not-found/"):
                raise ApiError(404, json={"error": {"code": ErrorCode.ItemNotFound, "message": uri}}) 

            if re.match(r"^/drives/[^/]+/$", uri):
                return {'@odata.context': 'https://graph.microsoft.com/v1.0/$metadata#drives/$entity', 'id': 'bdd46067213df13', 'name': 'drive-name', 'driveType': 'personal', 'owner': {'user': {'displayName': 'Atakama --', 'id': 'bdd46067213df13'}}, 'quota': {'deleted': 519205504, 'remaining': 1104878758982, 'state': 'normal', 'total': 1104880336896, 'used': 1577914}}

            err = ApiError(404, json={"error": {"code": ErrorCode.ItemNotFound, "message": "whatever"}}) 
            log.debug("raising %s", err)
            raise err

        if meth == "POST" and "/createUploadSession" in uri:
            self.called("upload.session", (uri,))
            log.debug("upload")
            return {'@odata.context': 'https://graph.microsoft.com/v1.0/$metadata#microsoft.graph.uploadSession', 'expirationDateTime': '2019-12-11T15:32:31.101Z', 'nextExpectedRanges': ['0-'], 'uploadUrl': upload_url}

        if meth == "PUT":
            self.called("upload.put", (uri,))
            return {"@odata.context":"https://graph.microsoft.com/v1.0/$metadata#drives('bdd46067213df13')/items/$entity", "@microsoft.graph.downloadUrl":"https://mckvog.bn.files.1drv.com/y4pxeIYeQKLFVu82R-paaa0e99SXlcC2zAz7ipLsi9EKUPVVsjUe-YBY2tXL6Uwr1KX4HP0tvg3kKejnhtmn79J8i6TW0-wYpdNvNCAKxAVi6UiBtIOUVtd75ZelLNsT_MpNzn65PdB5l926mUuPHq4Jqv3_FKdZCr0LmHm_QbbdEFenK3WgvDwFKIZDWCXEAdYxdJPqd2_wk0LVU9ClY4XBIcw84WPA1KdJbABz93ujiA", "createdDateTime":"2019-12-04T15:24:18.523Z", "cTag":"aYzpCREQ0NjA2NzIxM0RGMTMhMTAxMi4yNTc", "eTag":"aQkRENDYwNjcyMTNERjEzITEwMTIuMQ", "id":"BDD46067213DF13!1012", "lastModifiedDateTime":"2019-12-04T15:24:19.717Z", "name":"d943ae092dbf377dd443a9579eb10898.dest", "size":32, "webUrl":"https://1drv.ms/u/s!ABPfE3IGRt0Lh3Q", "createdBy":{"application":{"displayName":"Atakama", "id":"4423e6ce"}, "user":{"displayName":"Atakama --", "id":"bdd46067213df13"}}, "lastModifiedBy":{"application":{"displayName":"Atakama", "id":"4423e6ce"}, "user":{"displayName":"Atakama --", "id":"bdd46067213df13"}}, "parentReference":{"driveId":"bdd46067213df13", "driveType":"personal", "id":"BDD46067213DF13!1011", "name":"3676c7b907d09b2d9681084a47bcae59", "path":"/drive/root:/3676c7b907d09b2d9681084a47bcae59"}, "file":{"mimeType":"application/octet-stream", "hashes":{"quickXorHash":"MO4Q2k+0wIrVLvPvyFNEXjENmJU=", "sha1Hash":"9B628BE5312D2F5E7B6ADB1D0114BC49595269BE"}}, "fileSystemInfo":{"createdDateTime":"2019-12-04T15:24:18.523Z", "lastModifiedDateTime":"2019-12-04T15:24:19.716Z"}} # noqa

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
    root_event = {'@odata.type': '#microsoft.graph.driveItem', 'createdDateTime': '2019-10-27T05:46:04Z', 'id': '01LYWINUF6Y2GOVW7725BZO354PWSELRRZ', 'lastModifiedDateTime': '2020-01-02T15:45:55Z', 'name': 'root', 'webUrl': 'https://vidaid-my.sharepoint.com/personal/jack_vidaid_onmicrosoft_com/Documents', 'size': 6642351, 'parentReference': {'driveId': 'root', 'driveType': 'business'}, 'fileSystemInfo': {'createdDateTime': '2019-10-27T05:46:04Z', 'lastModifiedDateTime': '2020-01-02T15:45:55Z'}, 'folder': {'childCount': 5}, 'root': {}}
    non_root_event = {'@odata.type': '#microsoft.graph.driveItem', 'createdDateTime': '2020-01-02T15:44:44Z', 'eTag': '"{F0D504AA-C7E0-4B49-B529-63DEB72E09FE},1"', 'id': '01LYWINUFKATK7BYGHJFF3KKLD323S4CP6', 'lastModifiedDateTime': '2020-01-02T15:44:44Z', 'name': '20200102-02', 'webUrl': 'https://vidaid-my.sharepoint.com/personal/jack_vidaid_onmicrosoft_com/Documents/20200102-02', 'cTag': '"c:{F0D504AA-C7E0-4B49-B529-63DEB72E09FE},0"', 'size': 0, 'createdBy': {'application': {'displayName': 'Atakama'}, 'user': {'email': 'jack@vidaid.onmicrosoft.com', 'displayName': 'jack'}}, 'parentReference': {'driveId': 'root', 'driveType': 'business', 'id': '01LYWINUF6Y2GOVW7725BZO354PWSELRRZ', 'path': '/drive/root:'}, 'fileSystemInfo': {'createdDateTime': '2020-01-02T15:44:44Z', 'lastModifiedDateTime': '2020-01-02T15:44:44Z'}, 'folder': {'childCount': 2}}

    assert odp._convert_to_event(root_event, "123") is None
    assert odp._convert_to_event(non_root_event, "123") is not None


def test_namespace_get():
    _, odp = fake_odp()
    ns = odp.namespace
    nsid = odp.namespace_id
    assert ns
    assert nsid

def test_namespace_set():
    _, odp = fake_odp()
    odp.namespace = "personal"
    nsid = odp.namespace_id
    assert nsid

def test_namespace_multiple_personal_drives():
    srv, odp = fake_odp()
    srv.multiple_personal_drives = True
    odp.namespace = "personal/drive-2"
    nsid = odp.namespace_id
    assert nsid

def test_namespace_set_err():
    _, odp = fake_odp()
    with pytest.raises(CloudNamespaceError):
        odp.namespace = "bad-namespace"

def test_namespace_set_disconn():
    _, odp = fake_odp()
    odp.disconnect()
    with pytest.raises(CloudDisconnectedError):
        odp.namespace = "whatever"

def test_namespace_set_other():
    _, odp = fake_odp()

    def raise_error(a, b):
        raise CloudTokenError("yo")

    with patch.object(odp, '_direct_api', side_effect=raise_error):
        with pytest.raises(CloudTokenError):
            odp.namespace = "whatever"

def test_list_namespaces():
    api, odp = fake_odp()
    namespace_objs = odp.list_ns(recursive=False)
    namespaces = [ns.name for ns in namespace_objs]
    # personal is always there
    assert "personal" in namespaces
    # shared folders - fake namespace
    assert "shared" in namespaces
    # shared inner folder (parent is not root) is ignored
    assert "shared/user2_co_onmicrosoft_com/Documents" not in namespaces
    # shared file is ignored
    assert "shared/user3_co_onmicrosoft_com/Documents" not in namespaces
    # sites are listed
    assert "cloudsync-test-1" in namespaces
    assert "cloudsync-sub-site-1" in namespaces
    # protals are ignored
    assert "Community" not in namespaces
    # site fetch done only once
    assert len(api.calls["_fetch_sites"]) == 1
    # personal has no children
    child_namespaces = odp.list_ns(parent=namespace_objs[0])
    assert len(child_namespaces) == 0
    # shared has 2 children
    child_namespaces = odp.list_ns(parent=namespace_objs[1])
    assert len(child_namespaces) == 2

    # recursive
    api2, odp2 = fake_odp()
    namespaces = odp2.list_ns(recursive=True)
    # fetch additional info for 2 sites
    assert len(api2.calls["_fetch_sites"]) == 3

    #parent
    site = Namespace(name="name", id="site-id-1")
    children = odp2.list_ns(parent=site)
    assert not children
    site = Namespace(name="name", id="site-id-2")
    children = odp2.list_ns(parent=site)
    assert children

def test_drive_id_name_translation():
    _, odp = fake_odp()
    with pytest.raises(CloudFileNotFoundError):
        _ = odp._drive_id_to_name("item-not-found")
    assert odp._drive_id_to_name("blah") == "drive-name"

    with pytest.raises(CloudNamespaceError):
        _ = odp._drive_name_to_id("blah")
    assert odp._drive_name_to_id("cloudsync-test-1/sub-1")
    odp._fetch_drive_list(clear_cache=True)
    assert odp._drive_name_to_id("cloudsync-test-1/Community")
