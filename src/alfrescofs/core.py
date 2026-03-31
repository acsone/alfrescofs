import asyncio
import datetime as dt
import logging
import mimetypes
import os
import re
import threading
import weakref
from typing import Any, Dict, Literal, Optional, Union

import httpx
from authlib.integrations.httpx_client import AsyncOAuth2Client
from fsspec.asyn import (
    AbstractAsyncStreamedFile,
    AbstractBufferedFile,
    AsyncFileSystem,
    FSTimeoutError,
    sync,
    sync_wrapper,
)
from fsspec.utils import tokenize
from httpx import URL
from httpx._types import URLTypes

_logger = logging.getLogger(__name__)

HTTPX_RETRYABLE_ERRORS = (
    asyncio.TimeoutError,
    httpx.NetworkError,
    httpx.ProxyError,
    httpx.TimeoutException,
)

HTTPX_RETRYABLE_HTTP_STATUS_CODES = (502, 503, 504)


def get_running_loop():
    """Get the currently running event loop."""
    if hasattr(asyncio, "get_running_loop"):
        return asyncio.get_running_loop()
    loop = asyncio._get_running_loop()
    if loop is None:
        raise RuntimeError("no running event loop")
    return loop


def _guess_type(path: str) -> str:
    return mimetypes.guess_type(path)[0] or "application/octet-stream"


def _norm(path: str) -> str:
    path = (path or "").strip()
    if not path:
        return "/"
    if not path.startswith("/"):
        path = "/" + path
    while "//" in path:
        path = path.replace("//", "/")
    if len(path) > 1 and path.endswith("/"):
        path = path[:-1]
    return path


def parse_range_header(range_header: str) -> tuple:
    match = re.match(r"^bytes=(\d+)?-(\d+)?$", range_header.strip())
    if not match:
        raise ValueError(f"Invalid Range header: {range_header!r}")
    start = int(match.group(1)) if match.group(1) is not None else None
    end = int(match.group(2)) if match.group(2) is not None else None
    return start, end


def wrap_http_not_found_exceptions(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except httpx.HTTPStatusError as e:
            body = e.response.text
            if e.response.status_code == 404:
                raise FileNotFoundError(f"File not found: {e.request.url}") from e
            raise httpx.HTTPStatusError(
                f"HTTP {e.response.status_code} for {e.request.url}: {body}",
                request=e.request,
                response=e.response,
            ) from e

    return wrapper


@wrap_http_not_found_exceptions
async def _http_call_with_retry(
    func, *, args=(), kwargs=None, retries=5
) -> httpx.Response:
    kwargs = kwargs or {}
    for i in range(retries):
        try:
            r = await func(*args, **kwargs)
            r.raise_for_status()
            return r
        except HTTPX_RETRYABLE_ERRORS as e:
            if i == retries - 1:
                _logger.error("Request failed after %d retries: %s", retries, e)
                raise
            _logger.warning("Retryable error (attempt %d/%d): %s", i + 1, retries, e)
            await asyncio.sleep(min(1.7**i * 0.1, 15))
        except httpx.HTTPStatusError as e:
            if e.response.status_code in HTTPX_RETRYABLE_HTTP_STATUS_CODES:
                if i == retries - 1:
                    raise httpx.HTTPStatusError(
                        f"HTTP {e.response.status_code} for {e.request.url}"
                        f" (gave up after {retries} retries): {e.response.text}",
                        request=e.request,
                        response=e.response,
                    ) from e
                _logger.warning(
                    "Retryable HTTP %s (attempt %d/%d) for %s: %s",
                    e.response.status_code,
                    i + 1,
                    retries,
                    e.request.url,
                    e.response.text,
                )
                await asyncio.sleep(min(1.7**i * 0.1, 15))
                continue
            raise


def node(node_id: str, *parts: str) -> str:
    """Get Alfresco REST paths for node-based endpoints.

    Simple helper that allows higher-level code to not have to manually concatenate
    'nodes/{id}/...'.
    """
    node_id = node_id.strip("/")
    suffix = "/".join(p.strip("/") for p in parts if p)
    return f"nodes/{node_id}{'/' + suffix if suffix else ''}"


class AlfrescoFS(AsyncFileSystem):
    """Fsspec filesystem for Alfresco Content Services public REST API v1.

    At this point the query API has not been implemented, but it would
    not be too hard to do so.
    """

    protocol = ("alfresco", "alfd")
    retries = 5

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_path: str = "api/-default-/public/alfresco/versions/1",
        auth_type: Literal["basic", "oauth2"] = "basic",
        username: Optional[str] = None,
        password: Optional[str] = None,
        oauth2_client_params: Optional[dict] = None,
        asynchronous: bool = False,
        loop=None,
        verify: bool = True,
        timeout: float = 30.0,
        retries: int = 5,
        root_path: str = "/",
        content_app_url: Optional[str] = None,
        share_url: Optional[str] = None,
        **kwargs,
    ):
        from fsspec.asyn import get_loop

        super_kwargs = kwargs.copy()
        super_kwargs.pop("use_listings_cache", None)
        super_kwargs.pop("listings_expiry_time", None)
        super_kwargs.pop("max_paths", None)

        super().__init__(
            asynchronous=asynchronous, loop=loop or get_loop(), **super_kwargs
        )

        resolved_base_url = base_url or os.getenv("ALFRESCOFS_BASE_URL", "")
        if not resolved_base_url:
            raise ValueError(
                "base_url is required (or set ALFRESCOFS_BASE_URL environment variable)"
            )

        self._base_url = URL(resolved_base_url.rstrip("/"))
        self._api_root = self._base_url.join(api_path.rstrip("/") + "/")
        self._root_path = _norm(root_path)
        self._repo_root_path = None
        self._root_node_id = None

        self._auth_type = auth_type
        self._username = username or os.getenv("ALFRESCOFS_USERNAME")
        self._password = password or os.getenv("ALFRESCOFS_PASSWORD")

        if auth_type == "oauth2":
            env_oauth2 = {
                "client_id": os.getenv("ALFRESCOFS_CLIENT_ID"),
                "client_secret": os.getenv("ALFRESCOFS_CLIENT_SECRET"),
                "token_endpoint": os.getenv("ALFRESCOFS_TOKEN_ENDPOINT"),
                "scope": os.getenv("ALFRESCOFS_SCOPE"),
            }
            env_oauth2 = {k: v for k, v in env_oauth2.items() if v is not None}
            if oauth2_client_params:
                oauth2_client_params = {**env_oauth2, **oauth2_client_params}
            elif env_oauth2:
                oauth2_client_params = env_oauth2

        self._oauth2_params = oauth2_client_params
        self._verify = verify
        self._timeout = timeout
        self.retries = retries
        resolved_content_app_url = content_app_url or os.getenv(
            "ALFRESCOFS_CONTENT_APP_URL"
        )
        self._content_app_url = (
            resolved_content_app_url.rstrip("/") if resolved_content_app_url else None
        )
        resolved_share_url = share_url or os.getenv("ALFRESCOFS_SHARE_URL")
        self._share_url = resolved_share_url.rstrip("/") if resolved_share_url else None

        self._client: Optional[Union[httpx.AsyncClient, AsyncOAuth2Client]] = None
        self._client_lock = threading.Lock() if not asynchronous else None
        self._client_pid: Optional[int] = None

        self._repo_root = None

    def _get_loop(self):
        try:
            loop = get_running_loop()
        except RuntimeError:
            from fsspec.asyn import get_loop

            loop = get_loop()
            asyncio.set_event_loop(loop)
        return loop

    @property
    def loop(self):
        return self._get_loop()

    @property
    def client(self):
        current_pid = os.getpid()

        if self._client is None or self._client_pid != current_pid:
            if self.asynchronous:
                self._init_client()
                self._client_pid = current_pid
            else:
                with self._client_lock:
                    if self._client is None or self._client_pid != current_pid:
                        self._init_client()
                        self._client_pid = current_pid

        return self._client

    def _get_client(self):
        match self._auth_type:
            case "basic":
                if not self._username or not self._password:
                    raise ValueError(
                        "auth_type='basic' requires a username and password"
                    )

                return httpx.AsyncClient(
                    follow_redirects=True,
                    verify=self._verify,
                    timeout=httpx.Timeout(self._timeout),
                    auth=httpx.BasicAuth(self._username, self._password),
                )

            case "oauth2":
                if not self._oauth2_params:
                    raise ValueError("auth_type='oauth2' requires oauth2_client_params")

                return AsyncOAuth2Client(
                    follow_redirects=True,
                    verify=self._verify,
                    timeout=httpx.Timeout(self._timeout),
                    **self._oauth2_params,
                )

            case _:
                raise ValueError("You must define an authentication")

    def __del__(self):
        try:
            if hasattr(self, "_client") and self._client is not None:
                self.close_http_session(self._client, getattr(self, "loop", None))
        except Exception:
            pass

    def _init_client(self):
        if self._client is not None:
            try:
                self.close_http_session(self._client, getattr(self, "loop", None))
            except Exception:
                pass

        self._client = self._get_client()

        if not self.asynchronous:
            weakref.finalize(self, self.close_http_session, self._client, self.loop)

    @staticmethod
    def close_http_session(
        client: Union[httpx.AsyncClient, AsyncOAuth2Client],
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        if loop is not None and not loop.is_closed():
            try:
                if loop.is_running():
                    loop.create_task(client.aclose())
                    return
                sync(loop, client.aclose, timeout=0.1)
                return
            except (RuntimeError, FSTimeoutError, Exception):
                pass

    async def _ensure_root_initialized(self) -> None:
        if self._root_node_id is not None:
            return

        payload = await self._get_json(self._api_root.join(node("-root-")))
        repo_root_id = payload["entry"]["id"]

        root_path = _norm(self._root_path) if self._root_path else "/"

        if root_path in ("", "/"):
            self._root_node_id = repo_root_id
            self._root_path = "/"
            return

        rel = root_path.lstrip("/")
        payload2 = await self._get_json(
            self._api_root.join(node(repo_root_id)),
            params={"relativePath": rel},
        )
        self._root_node_id = payload2["entry"]["id"]
        self._root_path = root_path

    def _path_to_url(
        self,
        path: str | None = None,
        node_id: str | None = None,
        parts: tuple | None = None,
    ):
        if not node_id:
            if not path:
                raise ValueError("path or node_id required")
            node_id = sync(self.loop, self._path_to_node_id, path, True)
            if not node_id:
                raise FileNotFoundError(path)

        endpoint = node(node_id, *parts) if parts else node(node_id)

        return self._api_root.join(endpoint)

    async def _path_to_url_async(
        self,
        path: str | None = None,
        node_id: str | None = None,
        parts: tuple | None = None,
    ) -> URL:
        if not node_id:
            if not path:
                raise ValueError("path or node_id required")
            node_id = await self._path_to_node_id(path)
            if not node_id:
                raise FileNotFoundError(path)

        return self._path_to_url(node_id=node_id, parts=parts)

    async def _call_alf(self, method: str, url: URLTypes, **kwargs):
        if self._auth_type == "oauth2" and self.client.token is None:
            await self.client.fetch_token()

        return await _http_call_with_retry(
            self.client.request,
            args=(method, url),
            kwargs=kwargs,
            retries=self.retries,
        )

    async def _get(self, url: URLTypes, **kwargs):
        return await self._call_alf("GET", url, **kwargs)

    async def _post(self, url: URLTypes, **kwargs):
        return await self._call_alf("POST", url, **kwargs)

    async def _put(self, url: URLTypes, **kwargs):
        return await self._call_alf("PUT", url, **kwargs)

    async def _delete(self, url: URLTypes, **kwargs):
        return await self._call_alf("DELETE", url, **kwargs)

    async def _patch(self, url: URLTypes, **kwargs):
        return await self._call_alf("PATCH", url, **kwargs)

    async def _update_metadata(
        self,
        path: str,
        item_id: str | None = None,
        properties: dict | None = None,
        aspects: list[str] | None = None,
    ) -> None:
        node_id = item_id or await self._path_to_node_id(path)
        body: dict = {}
        if properties:
            body["properties"] = properties
        if aspects:
            body["aspectNames"] = aspects
        if not body:
            return
        url = self._api_root.join(node(node_id))
        await self._put(url, json=body)

    update_metadata = sync_wrapper(_update_metadata)

    async def _get_json(self, url: URLTypes, **kwargs):
        return (await self._get(url, **kwargs)).json()

    async def _path_to_node_id(self, path: str) -> Optional[str]:
        await self._ensure_root_initialized()

        path = _norm(self._strip_protocol(path))

        if path == "/":
            return self._root_node_id

        try:
            payload = await self._get_json(
                self._api_root.join(node(self._root_node_id)),
                params={"relativePath": path.lstrip("/")},
            )
        except FileNotFoundError:
            return None

        return payload.get("entry", {}).get("id")

    get_item_id = sync_wrapper(_path_to_node_id)

    def _get_relative_fs_path(self, entry: dict) -> str:
        if entry.get("id") == self._root_node_id:
            return "/"

        elements = (entry.get("path") or {}).get("elements") or []

        parts = []
        root_found = False

        for e in elements:
            if e.get("id") == self._root_node_id:
                root_found = True
                continue

            if root_found:
                parts.append(e["name"])

        parts.append(entry.get("name"))

        return "/" + "/".join(parts)

    def _build_weburl(self, node_id: str, type_: str) -> Optional[str]:
        if self._content_app_url:
            if type_ == "file":
                return (
                    f"{self._content_app_url}/#/personal-files/preview/file/{node_id}"
                )
            return f"{self._content_app_url}/#/nodes/{node_id}/children"
        if self._share_url:
            if type_ == "file":
                return (
                    f"{self._share_url}/page/document-details"
                    f"?nodeRef=workspace://SpacesStore/{node_id}"
                )
            return (
                f"{self._share_url}/page/folder-details"
                f"?nodeRef=workspace://SpacesStore/{node_id}"
            )
        return None

    def _node_entry_to_fsspec_info(self, entry: dict) -> dict:
        is_folder = (
            entry.get("isFolder") is True or entry.get("nodeType") == "cm:folder"
        )
        _type = "directory" if is_folder else "file"

        created = entry.get("createdAt") or "1970-01-01T00:00:00.000+0000"
        modified = entry.get("modifiedAt") or created

        def _parse_iso(s: str) -> dt.datetime:
            if s.endswith("+0000"):
                s = s[:-5] + "+00:00"
            return dt.datetime.fromisoformat(s)

        size = 0
        mimetype = None

        if _type == "file":
            content = entry.get("content") or {}
            size = int(content.get("sizeInBytes") or 0)
            mimetype = content.get("mimeType")

        data = {
            "name": self._get_relative_fs_path(entry),
            "size": size,
            "type": _type,
            "item_info": entry,
            "time": _parse_iso(created),
            "mtime": _parse_iso(modified),
            "id": entry.get("id"),
            "parentId": entry.get("parentId"),
        }

        if mimetype:
            data["mimetype"] = mimetype

        if "nodeType" in entry:
            data["nodeType"] = entry["nodeType"]

        if "aspectNames" in entry:
            data["aspects"] = entry["aspectNames"]

        if "properties" in entry:
            data["properties"] = entry["properties"]

        node_id = entry.get("id")
        if node_id:
            weburl = self._build_weburl(node_id, _type)
            if weburl:
                data["weburl"] = weburl

        return data

    async def _exists(self, path: str, **kwargs) -> bool:
        return (await self._path_to_node_id(path)) is not None

    @staticmethod
    def _build_include(extra: Union[str, list, None] = None) -> str:
        base = ["path"]
        if extra:
            extras = (
                [e.strip() for e in extra.split(",")]
                if isinstance(extra, str)
                else list(extra)
            )
            for e in extras:
                if e and e not in base:
                    base.append(e)
        return ",".join(base)

    async def _info(
        self,
        path: str,
        item_id: str | None = None,
        include: Union[str, list, None] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        await self._ensure_root_initialized()

        url = await self._path_to_url_async(path=path, node_id=item_id)

        payload = await self._get_json(
            url, params={"include": self._build_include(include)}
        )
        entry = payload.get("entry", {})

        return self._node_entry_to_fsspec_info(entry)

    async def _isdir(self, path: str) -> bool:
        try:
            return (await self._info(path))["type"] == "directory"
        except FileNotFoundError:
            return False

    async def _isfile(self, path: str) -> bool:
        try:
            return (await self._info(path))["type"] == "file"
        except FileNotFoundError:
            return False

    async def _fetch_children(
        self, url, include: Union[str, list, None] = None
    ) -> list[dict]:
        params: dict = {"include": self._build_include(include)}
        items = []
        while True:
            payload = await self._get_json(url, params=params)
            listing = payload.get("list", {})
            items.extend(e.get("entry", {}) for e in listing.get("entries", []))
            pagination = listing.get("pagination", {})
            if not pagination.get("hasMoreItems"):
                break
            params["skipCount"] = pagination.get("skipCount", 0) + pagination.get(
                "count", 0
            )
        return items

    async def _ls(
        self,
        path: str,
        detail: bool = True,
        item_id: str | None = None,
        include: Union[str, list, None] = None,
        **kwargs,
    ) -> list[dict | str]:
        if include and not detail:
            raise ValueError("include can only be used when detail=True")
        url = await self._path_to_url_async(
            path=path, node_id=item_id, parts=("children",)
        )
        items = await self._fetch_children(url, include=include)

        if not items:
            try:
                item = await self._info(path, item_id=item_id, **kwargs)
                if item["type"] == "file":
                    items = [item["item_info"]]
            except FileNotFoundError:
                pass

        if detail:
            return [self._node_entry_to_fsspec_info(e) for e in items if e.get("name")]

        base_path = _norm(self._strip_protocol(path)).rstrip("/")
        return [f"{base_path}/{e['name']}" for e in items if e.get("name")]

    async def _cat_file(
        self,
        path: str | None = None,
        item_id: str | None = None,
        start: int = None,
        end: int = None,
        **kwargs,
    ) -> bytes:
        nid = item_id or await self._path_to_node_id(path)
        headers = kwargs.get("headers", {}).copy()

        if start is not None or end is not None:
            if start is None:
                start = 0
            if end is None:
                headers["Range"] = f"bytes={start}-"
            else:
                headers["Range"] = f"bytes={start}-{max(start, end - 1)}"
        url = await self._path_to_url_async(node_id=nid, parts=("content",))
        r = await self._get(url, headers=headers)
        return r.content

    async def _get_file(self, rpath: str, lpath: str, **kwargs):
        data = await self._cat_file(rpath, **kwargs)
        with open(lpath, "wb") as f:
            f.write(data)

    async def _put_file(self, lpath: str, rpath: str, **kwargs):
        with open(lpath, "rb") as f:
            data = f.read()
        await self._pipe_file(rpath, data, **kwargs)

    async def _pipe_file(
        self,
        path: str,
        value: bytes,
        properties: dict | None = None,
        aspects: list[str] | None = None,
        **kwargs,
    ):
        if not isinstance(value, (bytes, bytearray)):
            raise TypeError(
                "alfrescofs currently supports bytes only for _pipe_file "
                "(streaming can be added later)"
            )

        path = _norm(self._strip_protocol(path))
        parent, name = path.rsplit("/", 1)
        parent = parent or "/"

        existing_nid = await self._path_to_node_id(path)
        if existing_nid:
            headers = kwargs.get("headers", {}).copy()
            headers.setdefault("Content-Type", _guess_type(path))
            url = await self._path_to_url_async(
                path=path, node_id=existing_nid, parts=("content",)
            )
            return await self._put(url, content=value, headers=headers)

        parent_nid = await self._path_to_node_id(parent)

        files = {"filedata": (name, value, _guess_type(name))}
        data = {"name": name, "nodeType": "cm:content"}
        if properties:
            data["properties"] = properties
        if aspects:
            data["aspectNames"] = aspects

        url = await self._path_to_url_async(
            path=path, node_id=parent_nid, parts=("children",)
        )

        return await self._post(url, files=files, data=data)

    async def _get_move_file_body(self, target_path: str):
        path = _norm(self._strip_protocol(target_path))

        parent, name = path.rsplit("/", 1)
        parent = parent or "/"

        nid = await self._path_to_node_id(parent)

        return {"targetParentId": nid, "name": name}

    async def _cp_file(
        self,
        origin_path: str,
        target_path: str,
        item_id: str | None = None,
        **kwargs,
    ):
        origin_path = _norm(self._strip_protocol(origin_path))
        nid = item_id or await self._path_to_node_id(origin_path)
        url = await self._path_to_url_async(node_id=nid, parts=("copy",))
        body = await self._get_move_file_body(target_path)

        return await self._post(url, json=body)

    async def _mv_file(
        self,
        origin_path: str,
        target_path: str,
        item_id: str | None = None,
        **kwargs,
    ):
        origin_path = _norm(self._strip_protocol(origin_path))
        nid = item_id or await self._path_to_node_id(origin_path)
        url = await self._path_to_url_async(node_id=nid, parts=("move",))
        body = await self._get_move_file_body(target_path)

        return await self._post(url, json=body)

    async def _copy(
        self,
        path1: str,
        path2: str,
        recursive: bool = False,
        maxdepth: int | None = None,
        on_error: str | None = None,
        **kwargs,
    ):
        await self._cp_file(path1, path2, **kwargs)

    cat_file = sync_wrapper(_cat_file)
    cp_file = sync_wrapper(_cp_file)
    mv_file = sync_wrapper(_mv_file)
    copy = sync_wrapper(_copy)
    mv = sync_wrapper(_mv_file)
    rename = sync_wrapper(_mv_file)

    async def _mkdir(
        self,
        path: str,
        create_parents: bool = True,
        exist_ok: bool = False,
        conflict_behavior: Literal["fail", "rename"] = "fail",
        properties: dict | None = None,
        aspects: list[str] | None = None,
        **kwargs,
    ):
        path = _norm(self._strip_protocol(path))

        if path == "/":
            return

        nid = await self._path_to_node_id(path)
        if nid is not None:
            if not exist_ok:
                raise FileExistsError(f"Directory already exists: {path}")
            return

        parent, name = path.rsplit("/", 1)
        parent = parent or "/"

        parent_nid = await self._path_to_node_id(parent)
        if parent_nid is None:
            if not create_parents:
                raise FileNotFoundError(f"Parent directory does not exist: {parent}")
            await self._mkdir(parent, create_parents=True, exist_ok=True)
            parent_nid = await self._path_to_node_id(parent)

        body: dict = {
            "name": name,
            "nodeType": "cm:folder",
        }
        if properties:
            body["properties"] = properties
        if aspects:
            body["aspectNames"] = aspects

        _logger.info(
            "_mkdir: creating %r under parent %s body=%s",
            path,
            parent_nid,
            body,
        )
        url = self._api_root.join(node(parent_nid, "children"))
        params = {"autoRename": "true"} if conflict_behavior == "rename" else {}

        try:
            response = await self._post(url, json=body, params=params)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409 and exist_ok:
                return None
            raise

        return response.json()["entry"]["id"]

    async def _makedirs(self, path: str, exist_ok: bool = False):
        await self._mkdir(path, create_parents=True, exist_ok=exist_ok)

    async def _touch(
        self,
        path: str,
        truncate: bool = False,
        properties: dict | None = None,
        aspects: list[str] | None = None,
        **kwargs,
    ):
        path = _norm(self._strip_protocol(path))
        if truncate or not await self._exists(path):
            await self._pipe_file(path, b"", properties=properties, aspects=aspects)

    touch = sync_wrapper(_touch)

    async def _modified(
        self, path: str, item_id: str | None = None, **kwargs
    ) -> dt.datetime:
        return (await self._info(path, item_id=item_id))["mtime"]

    modified = sync_wrapper(_modified)

    async def _created(
        self, path: str, item_id: str | None = None, **kwargs
    ) -> dt.datetime:
        return (await self._info(path, item_id=item_id))["time"]

    created = sync_wrapper(_created)

    async def _checksum(
        self, path: str, refresh: bool = False, item_id: str | None = None
    ) -> int:
        info = await self._info(path, item_id=item_id)
        if info["type"] == "file":
            return int(tokenize(info["id"], info["mtime"].isoformat()), 16)
        return int(tokenize(info), 16)

    checksum = sync_wrapper(_checksum)

    async def _get_versions(self, path: str, item_id: str | None = None) -> list[dict]:
        if not await self._isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
        nid = item_id or await self._path_to_node_id(path)
        url = self._api_root.join(node(nid, "versions"))
        params: dict = {}
        items = []
        while True:
            payload = await self._get_json(url, params=params)
            listing = payload.get("list", {})
            items.extend(e.get("entry", {}) for e in listing.get("entries", []))
            pagination = listing.get("pagination", {})
            if not pagination.get("hasMoreItems"):
                break
            params["skipCount"] = pagination.get("skipCount", 0) + pagination.get(
                "count", 0
            )
        return items

    get_versions = sync_wrapper(_get_versions)

    async def _checkout(self, path: str, item_id: str | None = None) -> None:
        if not await self._isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
        url = await self._path_to_url_async(path=path, node_id=item_id, parts=("lock",))
        await self._post(url, json={"type": "FULL", "lifetime": "PERSISTENT"})

    checkout = sync_wrapper(_checkout)

    async def _checkin(
        self,
        path: str,
        comment: str = "",
        item_id: str | None = None,
    ) -> None:
        if not await self._isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
        url = await self._path_to_url_async(
            path=path, node_id=item_id, parts=("unlock",)
        )
        await self._post(url)

    checkin = sync_wrapper(_checkin)

    async def _rm_file(self, path: str, item_id: str | None = None, **kwargs):
        nid = item_id or await self._path_to_node_id(path)
        url = await self._path_to_url_async(path=path, node_id=nid)
        await self._delete(url, params={"permanent": "true"})

    async def _rmdir(self, path: str, **kwargs):
        if not await self._isdir(path):
            raise FileNotFoundError(f"Directory not found: {path}")
        if await self._ls(path, detail=True):
            raise OSError(f"Directory not empty: {path}")
        nid = await self._path_to_node_id(path)
        url = await self._path_to_url_async(path=path, node_id=nid)
        await self._delete(url, params={"permanent": "true"})

    rmdir = sync_wrapper(_rmdir)

    async def _rm(self, path, recursive=False, batch_size=None, **kwargs):
        paths = path if isinstance(path, list) else [path]
        for p in paths:
            if await self._isdir(p):
                if not recursive and await self._ls(p, detail=True):
                    raise OSError(f"Directory not empty: {p}")
                if recursive:
                    items = await self._ls(p, detail=True)
                    dirs = [i["name"] for i in items if i["type"] == "directory"]
                    files = [i["name"] for i in items if i["type"] != "directory"]
                    for d in dirs:
                        await self._rm(
                            d, recursive=True, batch_size=batch_size, **kwargs
                        )
                    await self._rm_files(files, batch_size=batch_size, **kwargs)
                await self._rmdir(p, **kwargs)
            else:
                await self._rm_file(p, **kwargs)

    async def _rm_files(self, paths: list, batch_size=None, **kwargs):
        if not paths:
            return
        chunk = batch_size or len(paths)
        for i in range(0, len(paths), chunk):
            await asyncio.gather(
                *[self._rm_file(p, **kwargs) for p in paths[i : i + chunk]]
            )

    rm_files = sync_wrapper(_rm_files)

    async def _get_permissions(self, path: str, item_id: str | None = None) -> dict:
        info = await self._info(path, item_id=item_id, include="permissions")
        return info.get("item_info", {}).get("permissions", {})

    get_permissions = sync_wrapper(_get_permissions)

    async def _list_trash(self, max_items: int = 100) -> list[dict]:
        url = self._api_root.join("deleted-nodes")
        params: dict = {"maxItems": max_items}
        items = []
        while True:
            payload = await self._get_json(url, params=params)
            listing = payload.get("list", {})
            items.extend(e.get("entry", {}) for e in listing.get("entries", []))
            pagination = listing.get("pagination", {})
            if not pagination.get("hasMoreItems"):
                break
            params["skipCount"] = pagination.get("skipCount", 0) + pagination.get(
                "count", 0
            )
        return items

    list_trash = sync_wrapper(_list_trash)

    async def _restore(self, node_id: str) -> dict:
        url = self._api_root.join(f"deleted-nodes/{node_id}/restore")
        payload = await self._post(url)
        return payload.json().get("entry", {})

    restore = sync_wrapper(_restore)

    async def _purge(self, node_id: str) -> None:
        url = self._api_root.join(f"deleted-nodes/{node_id}")
        await self._delete(url)

    purge = sync_wrapper(_purge)

    async def _request_rendition(
        self,
        path: str,
        rendition_id: str = "pdf",
        item_id: str | None = None,
    ) -> None:
        if not await self._isfile(path):
            raise FileNotFoundError(f"Not a file: {path}")
        url = await self._path_to_url_async(
            path=path, node_id=item_id, parts=("renditions",)
        )
        await self._post(url, json={"id": rendition_id})

    request_rendition = sync_wrapper(_request_rendition)

    async def _get_rendition_content(
        self,
        path: str,
        rendition_id: str = "pdf",
        item_id: str | None = None,
    ) -> bytes:
        if not await self._isfile(path):
            raise FileNotFoundError(f"Not a file: {path}")
        url = await self._path_to_url_async(
            path=path,
            node_id=item_id,
            parts=("renditions", rendition_id, "content"),
        )
        response = await self._get(url)
        return response.content

    get_rendition_content = sync_wrapper(_get_rendition_content)

    def _open(
        self,
        path,
        mode="rb",
        block_size="default",
        cache_type="readahead",
        autocommit=True,
        size=None,
        cache_options=None,
        **kwargs,
    ):
        if ("r" in mode or "a" in mode) and not self.isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
        if "b" not in mode or kwargs.get("compression"):
            raise ValueError("binary mode only")
        if "r" in mode and size is None:
            size = self.size(path)

        return AlfrescoBufferedFile(
            fs=self,
            path=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            size=size,
            **kwargs,
        )

    async def open_async(self, path, mode="rb", **kwargs):
        if ("r" in mode or "a" in mode) and not await self._isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
        if "b" not in mode or kwargs.get("compression"):
            raise ValueError("binary mode only")
        size = None
        if "r" in mode or "a" in mode:
            size = (await self._info(path))["size"]
        return AlfrescoStreamedFile(self, path, mode, size=size, **kwargs)


class AlfrescoBufferedFile(AbstractBufferedFile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._node_id: str | None = None
        self._chunk_start: int = 0
        self._append_mode: bool = "a" in self.mode
        self._write_called: bool = False

    def _fetch_range(self, start, end) -> bytes:
        return self.fs.cat_file(self.path, start=start, end=end)

    def _init_write_append_mode(self):
        content = self.fs.cat_file(self.path)
        self.buffer.write(content)
        self.loc = len(content)

    def write(self, data):
        if self._append_mode and not self._write_called:
            self._init_write_append_mode()
        self._write_called = True
        return super().write(data)

    def _initiate_upload(self):
        if self.autocommit and self.tell() < self.blocksize:
            return
        nid = sync(self.fs.loop, self.fs._path_to_node_id, self.path)
        if not nid:
            self.fs.pipe_file(self.path, b"")
            nid = sync(self.fs.loop, self.fs._path_to_node_id, self.path)
        self._node_id = nid
        self._chunk_start = 0

    def _upload_chunk(self, final=False):
        if self._append_mode and not self._write_called:
            return False

        if self.autocommit and final and self.tell() < self.blocksize:
            self.buffer.seek(0)
            data = self.buffer.read()
            self.fs.pipe_file(self.path, data)
            return False

        self.buffer.seek(0)
        data = self.buffer.read(self.blocksize)
        while data:
            next_data = self.buffer.read(self.blocksize)
            is_last = final and not next_data
            start = self._chunk_start
            end = start + len(data) - 1
            total = start + len(data) if is_last else "*"
            headers = {
                "Content-Type": _guess_type(self.path),
                "Content-Range": f"bytes {start}-{end}/{total}",
            }
            url = self.fs._path_to_url(node_id=self._node_id, parts=("content",))
            sync(self.fs.loop, self.fs._put, url, content=data, headers=headers)
            self._chunk_start += len(data)
            data = next_data

        return not final


class AlfrescoStreamedFile(AbstractAsyncStreamedFile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._node_id: str | None = None
        self._chunk_start: int = 0
        self._append_mode: bool = "a" in self.mode
        self._write_called: bool = False

    async def _fetch_range(self, start, end) -> bytes:
        return await self.fs._cat_file(self.path, start=start, end=end)

    async def _init_write_append_mode(self):
        content = await self.fs._cat_file(self.path)
        self.buffer.write(content)
        self.loc = len(content)

    async def write(self, data):
        if self._append_mode and not self._write_called:
            await self._init_write_append_mode()
        self._write_called = True
        return await super().write(data)

    async def _initiate_upload(self):
        if self.autocommit and self.tell() < self.blocksize:
            return
        nid = await self.fs._path_to_node_id(self.path)
        if not nid:
            await self.fs._pipe_file(self.path, b"")
            nid = await self.fs._path_to_node_id(self.path)
        self._node_id = nid
        self._chunk_start = 0

    async def _upload_chunk(self, final=False):
        if self._append_mode and not self._write_called:
            return False

        if self.autocommit and final and self.tell() < self.blocksize:
            self.buffer.seek(0)
            data = self.buffer.read()
            await self.fs._pipe_file(self.path, data)
            return False

        self.buffer.seek(0)
        data = self.buffer.read(self.blocksize)
        while data:
            next_data = self.buffer.read(self.blocksize)
            is_last = final and not next_data
            start = self._chunk_start
            end = start + len(data) - 1
            total = start + len(data) if is_last else "*"
            headers = {
                "Content-Type": _guess_type(self.path),
                "Content-Range": f"bytes {start}-{end}/{total}",
            }
            url = await self.fs._path_to_url_async(
                node_id=self._node_id, parts=("content",)
            )
            await self.fs._put(url, content=data, headers=headers)
            self._chunk_start += len(data)
            data = next_data

        return not final
