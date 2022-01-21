"""
Onedrive provider
"""

# pylint: disable=missing-docstring

# https://dev.onedrive.com/
# https://docs.microsoft.com/en-us/onedrive/developer/rest-api/concepts/upload?view=odsp-graph-online
# https://docs.microsoft.com/en-us/onedrive/developer/rest-api/getting-started/msa-oauth?view=odsp-graph-online
# https://docs.microsoft.com/en-us/onedrive/developer/rest-api/getting-started/app-registration?view=odsp-graph-online
import os
import re
import logging
from pprint import pformat
import threading
import hashlib
import json
import enum
from typing import Generator, Optional, Dict, Iterable, List, Set, Union, cast, TYPE_CHECKING
import urllib.parse
from base64 import b64encode
from dataclasses import dataclass, field, fields
import time
import requests
import arrow

from cloudsync import Provider, Namespace, DIRECTORY, FILE, NOTKNOWN, Event, DirInfo
from cloudsync.exceptions import CloudTokenError, CloudDisconnectedError, CloudFileNotFoundError, \
    CloudFileExistsError, CloudCursorError, CloudTemporaryError, CloudNamespaceError
from cloudsync.oauth import OAuthConfig, OAuthProviderInfo
from cloudsync.registry import register_provider
from cloudsync.utils import debug_sig, memoize

import quickxorhash

if TYPE_CHECKING:
    from cloudsync import OInfo  # pragma: no cover

__version__ = "3.1.11"  # pragma: no cover

log = logging.getLogger(__name__)

QXHASH_0 = b"\0" * 20

SOCK_TIMEOUT = 180


class EventFilter(enum.Enum):
    """
    Event filter result
    """
    PROCESS = "process"
    IGNORE = "ignore"
    WALK = "walk"

    def __bool__(self):
        """
        Protect against bool use
        """
        raise ValueError("never bool enums")


class ErrorCode:
    #: Access was denied to the resource
    AccessDenied = "accessDenied"
    #: The activity limit has been reached
    ActivityLimitReached = "activityLimitReached"
    #: A general exception occured
    GeneralException = "generalException"
    #: An invalid range was provided
    InvalidRange = "invalidRange"
    #: An invalid request was provided
    InvalidRequest = "invalidRequest"
    #: The requested resource was not found
    ItemNotFound = "itemNotFound"
    #: Malware was detected in the resource
    MalwareDetected = "malwareDetected"
    #: The name already exists
    NameAlreadyExists = "nameAlreadyExists"
    #: The action was not allowed
    NotAllowed = "notAllowed"
    #: The action was not supported
    NotSupported = "notSupported"
    #: The resource was modified
    ResourceModified = "resourceModified"
    #: A resync is required
    ResyncRequired = "resyncRequired"
    #: The OneDrive service is not available
    ServiceNotAvailable = "serviceNotAvailable"
    #: The quota for this OneDrive has been reached
    QuotaLimitReached = "quotaLimitReached"
    #: The user is unauthenticated
    Unauthenticated = "unauthenticated"
    #: The response was malformed
    Malformed = "malformed"


class OneDriveInfo(DirInfo):
    pid: str = None
    path_orig: str = None

    def __init__(self, *a, pid=None, path_orig=None, **kws):
        """
        Adds "pid (parent id)" to the DirInfo
        """
        super().__init__(*a, **kws)
        self.pid = pid
        self.path_orig = path_orig


def _get_size_and_seek0(file_like):
    file_like.seek(0, os.SEEK_END)
    size = file_like.tell()
    file_like.seek(0)
    return size


@dataclass
class Drive(Namespace):
    parent: "Optional[Site]" = None
    url: str = ""
    owner: str = ""
    owner_type: str = ""
    owner_id: str = ""
    site_id: str = ""
    drive_id: str = ""
    shared_folder_id: str = ""
    shared_folder_path: str = ""

    @property
    def is_shared(self) -> bool:
        return bool(self.shared_folder_id)

    @property
    def api_root_oid(self) -> str:
        return self.shared_folder_id if self.is_shared else "root"

    @property
    def api_root_path(self) -> str:
        return f"items/{self.shared_folder_id}" if self.is_shared else "root"

    def __post_init__(self):
        if self.parent:
            self.parent.drives.append(self)
        ids = self.id.split("|")
        if len(ids) == 2:
            (self.site_id, self.drive_id) = ids
        elif len(ids) == 3:
            (self.site_id, self.drive_id, self.shared_folder_id) = ids
        else:
            self.drive_id = self.id

    def __repr__(self):
        d = {f.name: getattr(self, f.name) for f in fields(self) if f.name != "parent"}
        d["parent"] = self.parent.name if self.parent else None
        return str(d)


@dataclass
class Site(Namespace):
    drives: List[Drive] = field(default_factory=list)

    @property
    def is_parent(self) -> bool:
        return True

    @property
    def is_cached(self) -> bool:
        return bool(self.drives)


@dataclass
class _NamespaceErrors(Namespace):
    pass


NamespaceErrors = _NamespaceErrors("", "")


class OneDriveProvider(Provider):         # pylint: disable=too-many-public-methods, too-many-instance-attributes
    case_sensitive = False
    default_sleep = 15
    # Microsoft requests multiples of 320 KiB for upload_block_size
    # https://docs.microsoft.com/en-us/graph/api/driveitem-createuploadsession?view=graph-rest-1.0
    upload_block_size = 10 * 320 * 1024

    name = 'onedrive'
    _base_url = 'https://graph.microsoft.com/v1.0/'

    _oauth_info = OAuthProviderInfo(
        auth_url="https://login.microsoftonline.com/common/oauth2/v2.0/authorize?prompt=login",
        token_url="https://login.microsoftonline.com/common/oauth2/v2.0/token",
        scopes=['profile', 'openid', 'email', 'files.readwrite.all', 'sites.readwrite.all', 'offline_access'],
    )

    def __init__(self, oauth_config: Optional[OAuthConfig] = None):
        super().__init__()
        self._creds: Optional[Dict[str, str]] = None
        self.__cursor: Optional[str] = None
        self.__client: object = None
        self._mutex = threading.RLock()
        self._oauth_config = oauth_config
        self._auth_tokens: Optional[Dict[str, str]] = None
        self._namespace: Optional[Drive] = None
        self._personal_drive: Site = Site("Personal", "personal")
        self._shared_with_me: Site = Site("Shared With Me", "shared")
        self._namespace_errors: Site = Site("errors", "errors")
        self.__done_fetch_drive_list: bool = False
        self.__drive_by_id: Dict[str, Drive] = {}
        self.__site_by_id: Dict[str, Site] = {}
        self.__cached_is_biz = None
        self._http = requests.Session()

    @property
    def connected(self):
        return self.__client is not None

    def _api_path(self, *, oid: Optional[str]=None, path: Optional[str]=None) -> str:
        assert oid or path

        if oid == "root" or path == "/":
            oid = self.namespace.api_root_oid

        if oid:
            return f"/drives/{self._validated_namespace_id}/items/{oid}"

        enc_path = urllib.parse.quote(path)
        return f"/drives/{self._validated_namespace_id}/{self.namespace.api_root_path}:{enc_path}:"

    # names of args are compat with requests module
    def _direct_api(self, action, path=None, *, url=None, stream=None, data=None, headers=None,
            json=None, raw_response=False, timeout=SOCK_TIMEOUT):  # pylint: disable=redefined-outer-name
        assert path or url

        if not url:
            url = self._base_url.rstrip("/") + "/" + path.lstrip("/")

        with self._api():
            access_token = self._auth_tokens["access_token"]
            head = {
                'Authorization': f'bearer {access_token}',
                'content-type': 'application/json'
            }
            if headers:
                head.update(headers)
            for k, v in head.items():
                head[k] = str(v)
            log.debug("direct %s %s", action, url)
            req = self._http.request(
                action,
                url,
                stream=stream,
                headers=head,
                json=json,
                data=data,
                timeout=timeout
            )

        if raw_response:
            return req

        if req.status_code == 202:
            return {"location": req.headers.get("location", ""), "status_code": 202}

        if req.status_code == 204:
            return {}

        if req.status_code > 202:
            if not self._raise_converted_error(req):
                raise Exception(f"Unknown error {req.status_code} {req.json()}")

        if stream:
            return req

# very large: uncomment if more detail needed, semicolonn left in for lint prevention
#        log.debug("response %s", req.json());

        return req.json()

    def _direct_api_error_trap(self, path, method="get", default=None):
        # wrapper for _direct_api that raises only connectivity errors - other errors are caught and logged
        try:
            return self._direct_api(method, path)
        except (CloudDisconnectedError, CloudTokenError, CloudTemporaryError):
            raise
        except Exception as e:
            self._save_namespace_error(f"{path} failed with {repr(e)}")
            return default

    def _save_namespace_error(self, error: str):
        _ = Drive(id="", name=error, parent=self._namespace_errors)
        log.warning(error)

    def _save_drive_info(self, parent, drive_json):
        try:
            ids = f"{parent.id}|{drive_json['id']}"
            owner = drive_json.get("owner")
            owner_type = list(owner)[0] if owner else ""
            owner_id = owner[owner_type].get("id", "") if owner else ""
            owner_name = owner[owner_type].get("displayName", "") if owner else ""
            drive = Drive(f'{parent.name}/{drive_json.get("name", "Personal")}', ids,
                          parent=parent,
                          url=drive_json.get("webUrl"),
                          owner=owner_name,
                          owner_id=owner_id,
                          owner_type=owner_type)
            self.__drive_by_id[ids] = drive
        except Exception as e:
            self._save_namespace_error(f"Failed to parse drive json: {drive_json} {repr(e)}")

    def _save_shared_with_me_info(self, shared_json):
        try:
            remote_item = shared_json.get("remoteItem")
            if not remote_item or "folder" not in remote_item:
                # we only care about shared folders, not shared files
                return

            ids = f"{self._shared_with_me.id}|{remote_item['parentReference']['driveId']}|{remote_item['id']}"
            url = remote_item["webUrl"]
            shared = remote_item["shared"]
            shared_by = shared.get("sharedBy") or shared.get("owner")
            owner = (shared_by.get("user") or shared_by.get("group", {})).get("displayName")
            folder_name = remote_item.get("name", "")
            if self._is_biz:
                split_path = urllib.parse.unquote_plus(urllib.parse.urlparse(url).path).split('/')
                site_name = "Personal" if split_path[1] == "personal" else split_path[2]
                drive_name = split_path[3]
                name = f"Shared/{owner}/{site_name}/{drive_name}/{folder_name}"
            else:
                name = f"Shared/{owner}/Personal/{folder_name}"
            drive = Drive(name, ids,
                          parent=self._shared_with_me,
                          url=url,
                          owner=owner)
            self.__drive_by_id[ids] = drive
        except Exception as e:
            self._save_namespace_error(f"Failed to parse shared folder json: {shared_json} {repr(e)}")

    def _fetch_personal_drives(self):
        try:
            if self._personal_drive.drives:
                return

            # personal drive: "most users will only have a single drive resource" - Microsoft
            # see: https://docs.microsoft.com/en-us/graph/api/drive-list?view=graph-rest-1.0&tabs=http
            # we require at least one personal drive, but some users could have multiple
            drives = self._direct_api("get", "/me/drives")["value"]
            for drive in drives:
                self._save_drive_info(self._personal_drive, drive)
            if not self._personal_drive.drives:
                raise RuntimeError("no personal drive")
            if len(self._personal_drive.drives) == 1:
                self._personal_drive.drives[0].name = "Personal"
            else:
                self._personal_drive.drives.sort(key=lambda d: d.name.lower())
            self.__site_by_id[self._personal_drive.id] = self._personal_drive
            self.__cached_is_biz = drives[0]["driveType"] != "personal"
        except CloudDisconnectedError:
            raise
        except Exception as e:
            log.error("failed to get personal drive info: %s", repr(e))
            raise CloudTokenError("Invalid account, or no onedrive access")

    def _fetch_shared_drives(self):
        # drive items from other drives shared with current user
        shared = self._direct_api_error_trap("/me/drive/sharedWithMe", default={})
        for item in shared.get("value", []):
            self._save_shared_with_me_info(item)
        if self._shared_with_me.drives:
            self._shared_with_me.drives.sort(key=lambda d: d.name.lower())
            self.__site_by_id[self._shared_with_me.id] = self._shared_with_me

    def _fetch_sites(self):
        # sharepoint sites - a user can have access to multiple sites, with multiple drives in each
        if self._is_consumer:
            return
        sites = self._direct_api_error_trap("/sites?search=*", default={}).get("value", [])
        sites.sort(key=lambda s: (s.get("displayName") or s.get("name", "")).lower())
        for site in sites:
            try:
                # TODO: use configurable regex for filtering?
                url_path = urllib.parse.unquote_plus(urllib.parse.urlparse(site["webUrl"]).path).lower()
                if not url_path.startswith("/portals/"):
                    name = site.get("displayName") or site.get("name", "")
                    self.__site_by_id[site["id"]] = Site(name=name, id=site["id"])
            except Exception as e:
                self._save_namespace_error(f"Failed to parse site json: {site} {repr(e)}")

    def _fetch_drives_for_site(self, site: Site):
        # only sharepoint sites are lazy-loaded because they require one API hit per site
        # (a user could have access to a large number of sites)
        needs_fetch = site not in [self._personal_drive, self._shared_with_me] and not site.is_cached
        if needs_fetch:
            drives = self._direct_api_error_trap(f"/sites/{site.id}/drives", default={}).get("value", [])
            drives.sort(key=lambda sd: sd.get("name", "").lower())
            for drive in drives:
                self._save_drive_info(site, drive)
        return site.drives

    def _fetch_drive_list(self, clear_cache: bool = False):
        if clear_cache:
            self._personal_drive.drives = []
            self._shared_with_me.drives = []
            self._namespace_errors.drives = []
            self.__drive_by_id = {}
            self.__site_by_id = {}
            self.__done_fetch_drive_list = False
        if not self.__done_fetch_drive_list:
            self._fetch_personal_drives()
            self._fetch_shared_drives()
            self._fetch_sites()
            self.__done_fetch_drive_list = True

    def list_ns(self, recursive: bool = True, parent: Namespace = None) -> List[Namespace]:
        namespaces: List[Namespace] = []
        if parent:
            if isinstance(parent, _NamespaceErrors):
                namespaces += self._namespace_errors.drives
                return namespaces
            self._fetch_drive_list()
            site = self.__site_by_id.get(parent.id)
            if site:
                namespaces += self._fetch_drives_for_site(site)
            else:
                log.warning("Not a parent namespace: %s / %s", parent.id, parent.name)
        else:
            self._fetch_drive_list()
            namespaces += self._personal_drive.drives
            for _, site in self.__site_by_id.items():
                if site == self._personal_drive:
                    continue
                if recursive:
                    namespaces += self._fetch_drives_for_site(site)
                else:
                    namespaces.append(site)
        return namespaces

    @memoize
    def _check_ns(self, nsid, conn_id_for_memo):                                 # pylint: disable=unused-argument
        res = self._direct_api("get", f"/drives/{nsid}/items/{self.namespace.api_root_oid}", raw_response=True)
        return res.status_code < 300

    def _raise_converted_error(self, req):      # pylint: disable=too-many-branches, too-many-statements
        status = req.status_code
        try:
            dat = req.json()
            msg = dat["error"]["message"]
            code = dat["error"]["code"]
        except json.JSONDecodeError:
            msg = 'Bad Json'
            code = 'BadRequest'

        if status == 400 and not self._check_ns(self._validated_namespace_id, self.connection_id):
            raise CloudNamespaceError(msg)

        if status == -1 and "invalidclientquery" in str(code):
            raise CloudFileNotFoundError(msg)

        if status == 400 and code == -1 and "invalidclientquery" in str(code):
            # graph api can throw this if a child path isn't present as of 2020-03-15
            raise CloudFileNotFoundError(msg)

        if status < 300:
            log.error("Not converting err %s: %s", status, req)
            return False

        if status == 404:
            raise CloudFileNotFoundError(msg)
        if status == 410:
            raise CloudCursorError(msg)
        if status in (429, 503):
            raise CloudTemporaryError(msg)
        if code in ('ErrorInsufficientPermissionsInAccessToken', ErrorCode.Unauthenticated):
            self.disconnect()
            raise CloudTokenError(msg)
        if code == ErrorCode.Malformed:
            raise CloudFileNotFoundError(msg)
        if code == ErrorCode.ItemNotFound:
            raise CloudFileNotFoundError(msg)
        if code == ErrorCode.ResourceModified:
            raise CloudTemporaryError(msg)
        if code == ErrorCode.NameAlreadyExists:
            raise CloudFileExistsError(msg)
        if code == ErrorCode.AccessDenied:
            raise CloudFileExistsError(msg)
        if code == ErrorCode.NotSupported:
            raise CloudFileExistsError(msg)
        if status == 401:
            self.disconnect()
            raise CloudTokenError(msg)
        if code == "BadRequest":
            if status == 400:
                raise CloudFileNotFoundError(msg)
        if code == ErrorCode.InvalidRequest:
            if status == 405:
                raise CloudFileExistsError(msg)
            if status == 400:
                if "sync token" in msg.lower():
                    raise CloudCursorError(msg)
                raise CloudFileNotFoundError(msg)
        if code in ("UnknownError", "generalException"):
            raise CloudTemporaryError(msg)

        log.error("Not converting err %s", req)
        return False

    def get_quota(self):
        dat = self._direct_api("get", f"/drives/{self.namespace.drive_id}")
        log.debug("my drive %s", dat)
        total = dat.get("quota", {}).get("total", 0)
        remaining = dat.get("quota", {}).get("remaining", 0)
        return {
            'used': total - remaining,
            'limit': total,
            'login': self._personal_drive.drives[0].owner,
            'drive_id': dat['id'],
        }

    def reconnect(self):
        self.connect(self._creds)

    def connect_impl(self, creds):
        if not self.__client or creds != self._creds:
            log.info('Connecting to One Drive')

            with self._api(needs_client=False):
                try:
                    self._get_auth_tokens(creds)
                except requests.exceptions.ConnectionError:
                    raise CloudDisconnectedError("ConnectionError while authenticating")
                except Exception as e:
                    log.exception("exception while authenticating: %s", e)
                    raise CloudTokenError(str(e))

                self.__client = object()

                try:
                    self._fetch_drive_list(clear_cache=True)
                    if self.connection_id and self.connection_id != self._personal_drive.drives[0].drive_id:
                        raise CloudTokenError("OneDrive: cannot connect with mismatched credentials")
                    # validate namespace if specified, default to personal drive if not
                    self.namespace_id = self.namespace_id or self._personal_drive.drives[0].id
                except:
                    # any error in namespace fetch/validation leaves the provider in a bad state: no namespace or
                    # an unverified namespace. Consumers must reconnect to use this provider instance.
                    self.disconnect()
                    raise

        return self._personal_drive.drives[0].drive_id

    def _get_auth_tokens(self, creds: Dict[str, str]):
        if not creds:
            raise CloudTokenError("no credentials")

        refresh_token = creds.get("refresh", creds.get("refresh_token"))
        if not refresh_token:
            raise CloudTokenError("no refresh token, refusing connection")

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        params = {
            "refresh_token": refresh_token,
            "client_id": self._oauth_config.app_id,
            "redirect_uri": None,
            "grant_type": "refresh_token",
        }
        if self._oauth_config.app_secret is not None:
            params["client_secret"] = self._oauth_config.app_secret

        response = self._http.request("POST",
                                      self._oauth_info.token_url,
                                      headers=headers,
                                      data=params,
                                      timeout=SOCK_TIMEOUT)

        self._auth_tokens = json.loads(response.content)
        new_refresh_token = self._auth_tokens["refresh_token"]
        self._creds = {"refresh_token": new_refresh_token}
        if new_refresh_token != refresh_token:
            log.info("creds have changed")
            self._oauth_config.creds_changed(self._creds)

    def _api(self, *args, needs_client=True, **kwargs):  # pylint: disable=arguments-differ
        if needs_client and not self.__client:
            raise CloudDisconnectedError("currently disconnected")
        return self

    def __enter__(self):
        self._mutex.__enter__()
        return self.__client

    def __exit__(self, ty, ex, tb):
        self._mutex.__exit__(ty, ex, tb)

        if ex:
            try:
                raise ex
            except requests.ConnectionError as e:
                raise CloudDisconnectedError(f"cannot connect {e}")
            except (TimeoutError, ):
                self.disconnect()
                raise CloudDisconnectedError("disconnected on timeout")
            except IOError as e:
                raise CloudTemporaryError(f"io error {repr(e)}")
            except Exception:
                return False  # False allows the exit handler to act as normal, which does not swallow the exception
        return None

    def disconnect(self):
        with self._mutex:
            self.__client = None

    @property
    def latest_cursor(self):
        # see: https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_delta?view=odsp-graph-online#parameters
        # Note that for OneDrive enterprise the `delta` function only works for the root of a given drive, whereas
        # for OneDrive consumer it works for any given folder.
        api_root_path = "root" if self._is_biz else self.namespace.api_root_path
        res = self._direct_api("get", f"/drives/{self._validated_namespace_id}/{api_root_path}/delta?token=latest")
        return res.get('@odata.deltaLink')

    @property
    def current_cursor(self):
        if not self.__cursor:
            self.__cursor = self.latest_cursor
        return self.__cursor

    @current_cursor.setter
    def current_cursor(self, val):
        if val is None:
            val = self.latest_cursor
        if not isinstance(val, str) and val is not None:
            raise CloudCursorError(val)
        self.__cursor = val

    def _convert_to_event(self, change, new_cursor) -> Optional[Event]:
        # uncomment only while debugging, semicolon left in to cause linter to fail
        # log.debug("got event\n%s", pformat(change));

        # {'cTag': 'adDo0QUI1RjI2NkZDNDk1RTc0ITMzOC42MzcwODg0ODAwMDU2MDAwMDA',
        #  'createdBy': {'application': {'id': '4805d153'},
        #                'user': {'displayName': 'erik aronesty', 'id': '4ab5f266fc495e74'}},
        #  'createdDateTime': '2015-09-19T11:14:15.9Z', 'eTag': 'aNEFCNUYyNjZGQzQ5NUU3NCEzMzguMA',
        #  'fileSystemInfo': {
        #      'createdDateTime': '2015-09-19T11:14:15.9Z',
        #      'lastModifiedDateTime': '2015-09-19T11:14:15.9Z'},
        #  'folder': {'childCount': 0, 'folderType': 'document',
        #             'folderView': {'sortBy': 'name', 'sortOrder': 'ascending', 'viewType': 'thumbnails'}},
        #  'id': '4AB5F266FC495E74!338',
        #  'lastModifiedBy': {'application': {'id': '4805d153'}, 'user': {'displayName': 'erik aronesty', 'id': '4ab5f266fc495e74'}},
        #  'lastModifiedDateTime': '2019-11-08T22:13:20.56Z', 'name': 'root',
        #  'parentReference': {'driveId': '4ab5f266fc495e74', 'driveType': 'personal', 'id': '4AB5F266FC495E74!0', 'path': '/drive/root:'},
        #  'root': {}, 'size': 156, 'webUrl': 'https://onedrive.live.com/?cid=4ab5f266fc495e74'}
        if change['parentReference'].get('id') is None:
            log.debug("ignore event: drive root")
            return None

        ts = arrow.get(change.get('lastModifiedDateTime')).float_timestamp
        oid = change.get('id')

        # See:  https://docs.microsoft.com/en-us/graph/api/resources/deleted?view=graph-rest-1.0
        # Note that the "deleted" resource can return an empty dict, for example when a shared folder is "removed"
        # by the sharee -- still means the item was deleted in this case (the event does not contain a parent path).
        # In most cases the "delete" resource returns a dict containing a state:{ "state": "softDeleted" }
        exists = change.get('deleted') is None

        fil = change.get('file')
        fol = change.get('folder')
        if fil:
            otype = FILE
        elif fol:
            otype = DIRECTORY
        else:
            otype = NOTKNOWN

        log.debug("event %s", change)

        ohash = None
        path = None
        if exists:
            if otype == FILE:
                ohash = self._hash_from_dict(change)

            parent_path = change['parentReference'].get('path')

            if self._is_consumer and self.namespace.is_shared and not parent_path:
                # consumer OneDrive: shared folders generate events with an oid but no path --
                # as these folders are effectively drive roots, their events can be ignored
                return None

            path = self._join_parent_reference_path_and_name(parent_path, change['name'])
            if not path:
                # path is falsy when it is outside a shared folder (ODB only)
                return None

        return Event(otype, oid, path, ohash, exists, ts, new_cursor=new_cursor)

    def _filter_event(self, event: Event) -> EventFilter:
        # event filtering based on root path and event path

        if not event:
            return EventFilter.IGNORE

        if not self._root_path:
            return EventFilter.PROCESS

        state_path = self.sync_state.get_path(event.oid) if self.sync_state else None
        prior_subpath = self.is_subpath_of_root(state_path)
        if not event.exists:
            # delete - ignore if not in state, or in state but is not subpath of root
            return EventFilter.PROCESS if prior_subpath else EventFilter.IGNORE

        if event.path:
            curr_subpath = self.is_subpath_of_root(event.path)
            if curr_subpath and not prior_subpath:
                # Can't differentiate between creates and renames without watching the entire filesystem:
                # Event has an oid and a current path, its a rename if the oid was seen before,
                # but since events outside root are ignored we don't catch the case where an item is
                # created outside root and then renamed into root.
                # Hence the walk for directories -- a tradeoff for ignoring "outside root" events.
                log.debug("created in or renamed into root: %s", event.path)
                if event.otype == DIRECTORY:
                    return EventFilter.WALK
            elif prior_subpath and not curr_subpath:
                # Rename out of root: process the event.
                # Treated as a delete by the sync engine, which handles non-empty folders by marking
                # children "changed" and processing them first.
                log.debug("renamed out of root: %s", event.path)
            else:
                # both curr and prior are subpaths == rename within root (process event)
                # neither is subpath == rename outside root (ignore event)
                return EventFilter.PROCESS if curr_subpath else EventFilter.IGNORE

        return EventFilter.PROCESS

    def _walk_filtered_directory(self, oid: str, history: Set[str]):
        """
        Optimized walk for event filtering:

        When a folder is copied to/created in our sync root we get events for each child of that folder, but we also
        end up walking that folder recursively because there is no way to distinguish a copy/create (which does not
        require a walk) from a move (which does require a walk)

        Recursively walking (the traditional way) a folder with many child folders on copy/create thus presents
        a performance bottleneck -- we end up walking the child folders multiple times, since we get an event for
        each child folder.

        This modified recursive walk attempts to alleviate that somewhat by keeping track of walked oids, ensuring
        that a given folder is walked at most once per events() call.
        """
        if oid not in history:
            history.add(oid)
            try:
                for event in self.walk_oid(oid, recursive=False):
                    if event.otype == DIRECTORY:
                        yield from self._walk_filtered_directory(event.oid, history)
                    yield event
            except CloudFileNotFoundError:
                pass

    def events(self) -> Generator[Event, None, None]:      # pylint: disable=too-many-locals, too-many-branches
        page_token = self.current_cursor
        assert page_token
        done = False
        walk_history: Set[str] = set()

        while not done:
            # log.debug("looking for events, timeout: %s", timeout)
            res = self._direct_api("get", url=page_token)
            delta_link = res.get('@odata.deltaLink')
            next_link = res.get('@odata.nextLink')
            events: Union[List, Iterable] = res.get('value')
            new_cursor = next_link or delta_link

            if not self._is_biz:
                # events = sorted(events, key=lambda x: x["lastModifiedDateTime"]): # sorting by modtime also works
                events = reversed(cast(List, events))

            for change in events:
                event = self._convert_to_event(change, new_cursor)
                filter_result = self._filter_event(event)
                if filter_result == EventFilter.IGNORE:
                    continue
                if filter_result == EventFilter.WALK and event.otype == DIRECTORY:
                    log.debug("directory created in or renamed into root - walking: %s", event.path)
                    yield from self._walk_filtered_directory(event.oid, walk_history)
                yield event

            if new_cursor and page_token and new_cursor != page_token:
                self.__cursor = new_cursor
            page_token = new_cursor
            log.debug("new cursor %s", new_cursor)
            if delta_link:
                done = True

    def _hash_from_dict(self, change):
        if 'hashes' in change['file']:
            if self._is_biz:
                ohash = change['file']['hashes'].get('quickXorHash')
            else:
                ohash = change['file']['hashes'].get('sha1Hash')
            if ohash == "":
                ohash = None
        else:
            ohash = None
            if self._is_biz:
                if change['size'] == 0:
                    ohash = QXHASH_0
        if ohash is None:
            log.error("no hash for file? %s", pformat(change))
        return ohash

    def upload(self, oid, file_like, metadata=None) -> 'OInfo':
        size = _get_size_and_seek0(file_like)
        api_path = self._api_path(oid=oid)

        if size == 0:
            try:
                resp = self._direct_api("put", f"{api_path}/content", data=file_like)
            except CloudTemporaryError:
                # onedrive occasionally reports etag mismatch errors, even when there's no possibility of conflict
                # simply retrying here vastly reduces the number of false positive failures
                resp = self._direct_api("put", f"{api_path}/content", data=file_like)

            log.debug("uploaded: %s", resp.get("content"))
            return self._info_from_rest(resp)
        else:
            info = self.info_oid(oid)
            if not info:
                raise CloudFileNotFoundError("Uploading to nonexistent oid")

            if info.otype == DIRECTORY:
                raise CloudFileExistsError("Trying to upload on top of directory")

            _unused_resp = self._upload_large(api_path, file_like, "replace")
            # todo: maybe use the returned item dict to speed this up
            return self.info_oid(oid)

    def create(self, path, file_like, metadata=None) -> 'OInfo':
        if not metadata:
            metadata = {}

        pid = self._get_parent_id(path=path)
        dirname, base = self.split(path)
        size = _get_size_and_seek0(file_like)

        if size == 0:
            if self.exists_path(path):
                raise CloudFileExistsError()

            base = base.replace("'", "''")
            name = urllib.parse.quote(base)
            api_path = self._api_path(oid=pid) + "/children('" + name + "')/content"
            try:
                headers = {'content-type': 'text/plain'}
                r = self._direct_api("put", api_path, data=file_like, headers=headers)  # default timeout ok, size == 0 from "if" condition
                return self._info_from_rest(r, root=dirname)
            except CloudTemporaryError:
                info = self.info_path(path)
                # onedrive can fail with ConnectionResetByPeer, but still secretly succeed... just without returning info
                # if so, check hashes, and if all is OK, return OK
                if info and info.hash == self.hash_data(file_like):
                    return info
                # alternately this could be a race condition, where two people upload at once
                # so fail otherwise
                raise

        else:
            api_path = self._api_path(path=path)
            r = self._upload_large(api_path, file_like, conflict="fail")
            return self._info_from_rest(r, root=self.dirname(path))

    def _upload_large(self, drive_path, file_like, conflict):  # pylint: disable=too-many-locals
        size = _get_size_and_seek0(file_like)
        with self._api():
            r = self._direct_api("post", f"{drive_path}/createUploadSession", json={"item": {"@microsoft.graph.conflictBehavior": conflict}})
            upload_url = r["uploadUrl"]

            data = file_like.read(self.upload_block_size)

            max_retries_per_block = 10

            cbfrom = 0
            retries = 0
            while data:
                clen = len(data)             # fragment content size
                cbto = cbfrom + clen - 1     # inclusive content byte range
                cbrange = f"bytes {cbfrom}-{cbto}/{size}"
                try:
                    headers = {"Content-Length": clen, "Content-Range": cbrange}
                    r = self._direct_api("put", url=upload_url, data=data, headers=headers)
                except (CloudDisconnectedError, CloudTemporaryError) as e:
                    retries += 1
                    log.exception("Exception during _upload_large, continuing, range=%s, exception%s: %s", cbrange, retries, type(e))
                    if retries >= max_retries_per_block:
                        raise e
                    continue

                data = file_like.read(self.upload_block_size)
                cbfrom = cbto + 1
                retries = 0
            return r

    def download(self, oid, file_like):
        api_path = self._api_path(oid=oid) + "/content"
        r = self._direct_api("get", api_path, stream=True)
        for chunk in r.iter_content(chunk_size=4096):
            file_like.write(chunk)
            file_like.flush()

    def rename(self, oid, path):  # pylint: disable=too-many-locals, too-many-branches, too-many-statements
        with self.api():
            self._verify_parent_folder_exists(path)
            parent, base = self.split(path)

            info = self.info_oid(oid)
            if not info:
                raise CloudFileNotFoundError(f"oid not found:{oid}")

            api_path = self._api_path(oid=oid)
            old_path = info.path
            old_parent_id = info.pid
            new_parent_info = self.info_path(parent)
            new_parent_id = self.globalize_oid(new_parent_info.oid)

            # support copy over an empty folder
            if info.otype == DIRECTORY:
                try:
                    target_info = self.info_path(path)
                except CloudFileNotFoundError:
                    target_info = None
                if target_info and target_info.otype == DIRECTORY and target_info.oid != oid:
                    is_empty = True
                    for _ in self.listdir(target_info.oid):
                        is_empty = False
                        break
                    if is_empty:
                        self.delete(target_info.oid)

            rename_json = {}
            if info.name != base:
                rename_json["name"] = base
                need_temp = info.path.lower() == path.lower()
                if need_temp:
                    temp_json = {"name": base + os.urandom(8).hex()}
                    self._direct_api("patch", api_path, json=temp_json)
            if old_parent_id != new_parent_id:
                rename_json["parentReference"] = {"id": new_parent_id}
            if not rename_json:
                return oid
            ret = self._direct_api("patch", api_path, json=rename_json)
            if ret.get("status_code", 0) == 202:
                # wait for move/copy to complete to get the new oid
                new_oid = None
                for i in range(5):
                    time.sleep(i)
                    info = self.info_path(path)
                    if info:
                        new_oid = info.oid
                        break
                if not new_oid:
                    log.error("oid lookup failed after move/copy")
                    raise CloudFileNotFoundError("oid lookup failed after move/copy")
                oid = new_oid

            new_path = self.info_oid(oid).path
            if self.paths_match(old_path, new_path, for_display=True): # pragma: no cover
                log.error("rename did not change cloud file path: old=%s new=%s", old_path, new_path)
                raise CloudTemporaryError("rename did not change cloud file path")

            return oid

    @staticmethod
    def _parse_time(time_str):
        try:
            if time_str:
                ret_val = arrow.get(time_str).int_timestamp
            else:
                ret_val = 0
        except Exception as e:  # pragma: no cover
            log.error("could not convert time string '%s' to timestamp: %s", time_str, e)
            ret_val = 0
        return ret_val

    def _make_path_relative_to_shared_folder_if_needed(self, path, force=False):
        if self._is_biz and self.namespace and self.namespace.is_shared:
            relative_path = self.is_subpath(self.namespace.shared_folder_path, path)
            if relative_path or force:
                return relative_path
        return path

    def _info_from_rest(self, item, root=None):
        if not root:
            # parentReference.path format is: "drives/{drive_id}/root:/path/to/file"
            # To get the file path, everything up to and including the first ":" can be ignored
            # See: https://docs.microsoft.com/en-us/graph/api/resources/itemreference?view=graph-rest-1.0
            # Note:
            #   - if parentReference.path is missing -- item is the drive root dir
            #   - if parentReference.path == "drives/{drive_id}/root:" -- parent is the drive root dir
            parent_ref = item["parentReference"]
            if "path" in parent_ref:
                root = urllib.parse.unquote(parent_ref["path"].split(":", 1)[1]) or "/"

        name = item["name"]
        path_orig = self.join(root, name) if root else "/"
        path = self._make_path_relative_to_shared_folder_if_needed(path_orig)

        iid = item["id"]
        ohash = None
        if "folder" in item:
            otype = DIRECTORY
        else:
            otype = FILE
        if "file" in item:
            ohash = self._hash_from_dict(item)

        pid = item["parentReference"].get("id")
        size = item.get("size", 0)
        mtime = item["lastModifiedDateTime"]
        mtime = mtime and self._parse_time(mtime)
        shared = False
        if "createdBy" in item:
            shared = bool(item.get("remoteItem"))

        return OneDriveInfo(oid=iid, otype=otype, hash=ohash, path=path, path_orig=path_orig, pid=pid, name=name,
                            size=size, mtime=mtime, shared=shared)

    def listdir(self, oid) -> Generator[OneDriveInfo, None, None]:
        api_path = self._api_path(oid=oid) + "/children"
        res = self._direct_api("get", api_path)
        items = res.get("value", [])
        next_link = res.get("@odata.nextLink")

        while items:
            for item in items:
                yield self._info_from_rest(item)

            items = []
            if next_link:
                res = self._direct_api("get", url=next_link)
                items = res.get("value", [])
                next_link = res.get("@odata.nextLink")

    def mkdir(self, path, metadata=None) -> str:    # pylint: disable=arguments-differ
        _ = metadata
        log.debug("mkdir %s", path)

        # boilerplate: probably belongs in base class
        info = self.info_path(path)
        if info:
            if info.otype == FILE:
                raise CloudFileExistsError(path)
            log.info("Skipped creating already existing folder: %s", path)
            return info.oid

        pid = self._get_parent_id(path=path)
        log.debug("got pid %s", pid)

        _, new_folder = self.split(path)
        api_path = self._api_path(oid=pid)

        log.debug("mkdir parent_path=%s", api_path)
        data = {
            "name": new_folder,
            "folder": {},
            "@microsoft.graph.conflictBehavior": "fail"
        }
        res = self._direct_api("post", f"{api_path}/children", json=data)
        oid = res.get("id")
        if not oid:
            raise CloudFileExistsError(f"failed to mkdir: {path}")
        return oid

    def delete(self, oid):
        try:
            with self._api():
                info = self.info_oid(oid)
                if not info:
                    log.info("deleted non-existing oid %s", debug_sig(oid))  # pragma: no cover
                    return  # file doesn't exist already...
                if info.otype == DIRECTORY:
                    try:
                        next(self.listdir(oid))
                        raise CloudFileExistsError(f"Cannot delete non-empty folder {oid}:{info.name}")
                    except StopIteration:
                        pass  # Folder is empty, delete it no problem
                api_path = self._api_path(oid=oid)
                self._direct_api("delete", api_path)
        except CloudFileNotFoundError:
            pass

    def globalize_oid(self, oid: str) -> str:
        return self.info_oid(oid).oid if oid == "root" else oid

    def exists_oid(self, oid):
        return self.info_oid(oid) is not None

    def info_path(self, path: str, use_cache=True) -> Optional[OneDriveInfo]:
        log.debug("info path %s", path)
        try:
            if path == "/":
                return OneDriveInfo(oid="root", otype=DIRECTORY, hash=None, path="/", path_orig="/", pid=None, name="",
                                    mtime=None, shared=False)

            api_path = self._api_path(path=path)
            res = self._direct_api("get", api_path)
            return self._info_from_rest(res, root=self.dirname(path))
        except CloudFileNotFoundError:
            return None

    def exists_path(self, path) -> bool:
        try:
            return bool(self.info_path(path))
        except CloudFileNotFoundError:
            return False

    def _get_parent_id(self, *, path=None, oid=None):
        log.debug("get parent %s", path)
        if not path and not oid:
            log.error("invalid info %s %s", path, oid)
            raise CloudFileNotFoundError("Invalid path/oid")

        ret = None

        if path:
            ppath = self.dirname(path)
            i = self.info_path(ppath)
            if i:
                ret = i.oid
                if i.otype == FILE:
                    raise CloudFileExistsError("file where a folder should be")

        if oid is not None:
            i = self.info_oid(oid)
            if i:
                ret = i.pid     # parent id

        if not ret:
            raise CloudFileNotFoundError(f"parent {ppath} must exist")

        return ret

    def _join_parent_reference_path_and_name(self, pr_path, name):
        assert pr_path
        path = self.join(pr_path, name)
        preambles = [r"/drive/root:", r"/me/drive/root:", r"/drives/.*?/root:", f"/drives/.*?/{self.namespace.api_root_path}:"]

        if ':' in path:
            found = False
            for preamble in preambles:
                m = re.match(preamble, path)
                if m:
                    pre = m[0]
                    path = path[len(pre):]
                    found = True
                    break
            if not found:
                raise Exception(f"path '{path}'({pr_path}, {name}) does not start with '{preambles}', maybe implement recursion?")

        path = urllib.parse.unquote(path)
        path = self._make_path_relative_to_shared_folder_if_needed(path, force=True)
        return path

    def info_oid(self, oid: str, use_cache=True) -> Optional[OneDriveInfo]:
        try:
            api_path = self._api_path(oid=oid)
            res = self._direct_api("get", api_path)
            return self._info_from_rest(res)
        except CloudFileNotFoundError:
            return None

    def hash_data(self, file_like) -> str:
        # get a hash from a filelike that's the same as the hash i natively use
        if self._is_biz:
            h = quickxorhash.quickxorhash()
            for c in iter(lambda: file_like.read(32768), b''):
                h.update(c)
            return b64encode(h.digest()).decode("utf8")
        else:
            h = hashlib.sha1()
            for c in iter(lambda: file_like.read(32768), b''):
                h.update(c)
            return h.hexdigest().upper()

    @property
    def namespace(self) -> Optional[Drive]:
        return self._namespace

    @namespace.setter
    def namespace(self, ns: Drive):
        self.namespace_id = ns.id

    @property
    def _is_biz(self):
        if self.__cached_is_biz is None:
            dat = self._direct_api("get", f"/drives/{self._validated_namespace_id}/")
            self.__cached_is_biz = dat["driveType"] != 'personal'
        return self.__cached_is_biz

    @property
    def _is_consumer(self):
        return not self._is_biz

    @property
    def _validated_namespace_id(self):
        if self.connected and self.namespace_id:
            return self.namespace.drive_id
        raise CloudNamespaceError("namespace_id has not been validated")

    @property
    def namespace_id(self) -> Optional[str]:
        return self._namespace.id if self._namespace else None

    @namespace_id.setter
    def namespace_id(self, ns_id: str):
        if self.connected:
            # validate
            try:
                self._namespace = self._get_validated_namespace(ns_id)
            except:
                self._namespace = None
                raise

            if self.namespace.is_shared:
                self.namespace.shared_folder_path = self.info_oid(self.namespace.shared_folder_id).path_orig
                log.info("namespace.shared_folder_path = %s", self.namespace.shared_folder_path)
        else:
            # defer validation until a connection is established
            self._namespace = self.__drive_by_id.get(ns_id, Drive(name=ns_id, id=ns_id))
        log.info("USING NS name=%s id=%s - connected=%s", self.namespace.name, self.namespace_id, self.connected)

    def _get_validated_namespace(self, ns_id: str):
        drive = self.__drive_by_id.get(ns_id)
        if not drive:
            self._fetch_drive_list()
            drive = self.__drive_by_id.get(ns_id)
        if not drive:
            ids = Drive(ns_id, ns_id)
            if ids.site_id:
                site = self.__site_by_id.get(ids.site_id)
                if not site:
                    raise CloudNamespaceError(f"Unknown site id: {ns_id}")
                self._fetch_drives_for_site(site)
                drive = self.__drive_by_id.get(ns_id)
                if not drive and site == self._shared_with_me and not ids.is_shared and self._is_biz:
                    # backwards compatibility for legacy shared folder namespaces (ODB only)
                    drive = next((d for d in self._shared_with_me.drives if d.drive_id == ids.drive_id), None)
                    if drive:
                        name = "/".join(drive.name.split("/")[:-1])
                        drive = Drive(name=name, id=ns_id)
                if not drive:
                    raise CloudNamespaceError(f"Site does not contain drive: {ns_id}")
            elif ids.drive_id:
                drive = next((d for d in self._personal_drive.drives if d.drive_id == ids.drive_id), None)
                if not drive:
                    try:
                        api_drive = self._direct_api("get", f"/drives/{ids.drive_id}/")
                    except Exception as e:
                        raise CloudNamespaceError(f"Drive fetch error: {repr(e)}")
                    drive = Drive(api_drive.get("name", "Personal"), ns_id)
            else:
                raise CloudNamespaceError(f"Malformed drive id: {ns_id}")
        return drive

    @classmethod
    def test_instance(cls):
        return cls.oauth_test_instance(prefix=cls.name.upper(), port_range=(54200, 54210), host_name="localhost")

    @property
    def _test_namespace(self) -> Namespace:
        return self._personal_drive.drives[0]


class OneDriveBusinessTestProvider(OneDriveProvider):
    name = "testodbiz"


register_provider(OneDriveBusinessTestProvider)

__cloudsync__ = OneDriveProvider
