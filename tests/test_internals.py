from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alfrescofs import AlfrescoFS

BASE_URL = "http://mock-internals.test:8080"
ROOT_ID = "root-node-id"
NODE_ID = "file-node-id"
CONTENT_APP_URL = "https://alfresco.example.com"
BLOCKSIZE = 10


def make_fs(content_app_url=None) -> AlfrescoFS:
    """Create a fresh FS instance with optional content app URL."""
    AlfrescoFS.clear_instance_cache()
    fs = AlfrescoFS(
        base_url=BASE_URL,
        auth_type="basic",
        username="u",
        password="p",
        content_app_url=content_app_url,
    )
    fs._root_node_id = ROOT_ID
    return fs


# ---------------------------------------------------------------------------
# Include handling
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "extra, expected",
    [
        (None, "path"),
        ("properties", "path,properties"),
        (["properties", "permissions"], "path,properties,permissions"),
        ("path,properties", "path,properties"),
    ],
)
def test_build_include(extra, expected):
    """'path' is always included exactly once; extras are appended."""
    assert AlfrescoFS._build_include(extra) == expected


async def test_ls_include_without_detail_raises():
    """Include requires detail=True — otherwise invalid."""
    fs = make_fs()

    with pytest.raises(ValueError, match="include"):
        await fs._ls("/", detail=False, include="properties")


# ---------------------------------------------------------------------------
# Info / listing API contract
# ---------------------------------------------------------------------------


MINIMAL_PAYLOAD = {
    "entry": {
        "id": NODE_ID,
        "name": "file.txt",
        "isFolder": False,
        "nodeType": "cm:content",
        "createdAt": "2024-01-01T00:00:00.000+0000",
        "modifiedAt": "2024-01-01T00:00:00.000+0000",
        "parentId": ROOT_ID,
        "content": {"sizeInBytes": 10, "mimeType": "text/plain"},
        "path": {"elements": [{"id": ROOT_ID, "name": "Company Home"}]},
    }
}


async def test_info_include_forwarded_to_http_params():
    """_info forwards include to HTTP params."""
    fs = make_fs()

    with patch.object(
        fs, "_get_json", new_callable=AsyncMock, return_value=MINIMAL_PAYLOAD
    ) as mock_get:
        with patch.object(
            fs, "_path_to_url_async", new_callable=AsyncMock, return_value="http://fake"
        ):
            await fs._info("/file.txt", include="properties")

    assert mock_get.call_args[1]["params"]["include"] == "path,properties"


async def test_fetch_children_include_forwarded_to_http_params():
    """_fetch_children forwards include to HTTP params."""
    list_payload = {"list": {"entries": [], "pagination": {"hasMoreItems": False}}}
    fs = make_fs()

    with patch.object(
        fs, "_get_json", new_callable=AsyncMock, return_value=list_payload
    ) as mock_get:
        await fs._fetch_children("http://fake/children", include="aspectNames")

    assert mock_get.call_args[1]["params"]["include"] == "path,aspectNames"


# ---------------------------------------------------------------------------
# Write (small vs large files)
# ---------------------------------------------------------------------------


async def test_small_file_uses_pipe_file():
    """Small writes use pipe_file (single request)."""
    fs = make_fs()
    data = b"hello"

    with patch.object(fs, "pipe_file") as mock_pipe:
        with fs.open("/file.txt", "wb", block_size=BLOCKSIZE) as f:
            f.write(data)

    mock_pipe.assert_called_once_with("/file.txt", data)


async def test_large_file_content_range_headers():
    """Large writes send chunks with correct Content-Range headers."""
    fs = make_fs()
    captured_ranges = []

    async def fake_put(url, **kwargs):
        captured_ranges.append(kwargs.get("headers", {}).get("Content-Range", ""))
        return MagicMock()

    with patch.object(
        fs, "_path_to_node_id", new_callable=AsyncMock, return_value=NODE_ID
    ):
        with patch.object(fs, "_put", side_effect=fake_put):
            with patch.object(
                fs,
                "_path_to_url_async",
                new_callable=AsyncMock,
                return_value="http://fake",
            ):
                async with await fs.open_async(
                    "/file.txt", "wb", block_size=BLOCKSIZE
                ) as f:
                    await f.write(b"A" * BLOCKSIZE)
                    await f.write(b"B" * BLOCKSIZE)
                    await f.write(b"C" * 3)

    total = BLOCKSIZE * 2 + 3
    assert captured_ranges[0] == f"bytes 0-{BLOCKSIZE - 1}/*"
    assert captured_ranges[1] == f"bytes {BLOCKSIZE}-{BLOCKSIZE * 2 - 1}/*"
    assert captured_ranges[2] == f"bytes {BLOCKSIZE * 2}-{total - 1}/{total}"


async def test_large_file_creates_node_when_missing():
    """Upload creates empty node if target does not exist."""
    fs = make_fs()
    nid_calls = [0]

    async def fake_nid(path):
        nid_calls[0] += 1
        return None if nid_calls[0] == 1 else NODE_ID

    with patch.object(fs, "_path_to_node_id", side_effect=fake_nid):
        with patch.object(fs, "_pipe_file", new_callable=AsyncMock) as mock_pipe:
            with patch.object(
                fs, "_put", new_callable=AsyncMock, return_value=MagicMock()
            ):
                with patch.object(
                    fs,
                    "_path_to_url_async",
                    new_callable=AsyncMock,
                    return_value="http://fake",
                ):
                    async with await fs.open_async(
                        "/file.txt", "wb", block_size=BLOCKSIZE
                    ) as f:
                        await f.write(b"A" * (BLOCKSIZE + 1))

    mock_pipe.assert_called_once_with("/file.txt", b"")


# ---------------------------------------------------------------------------
# _pipe_file with properties / aspects
# ---------------------------------------------------------------------------

PIPE_PARENT_ID = "parent-node-id"
PIPE_POST_RESPONSE = {"entry": {"id": "new-file-id", "name": "file.txt"}}


def _pipe_nid_side_effect(path):
    """Return None for the new file, PIPE_PARENT_ID for its parent."""
    return None if path == "/file.txt" else PIPE_PARENT_ID


async def test_pipe_file_with_properties_and_aspects():
    """Properties and aspects are forwarded in the POST data for new nodes."""
    fs = make_fs()
    captured = {}

    async def fake_post(url, **kwargs):
        captured.update(kwargs.get("data", {}))
        return MagicMock(json=lambda: PIPE_POST_RESPONSE)

    with patch.object(fs, "_path_to_node_id", side_effect=_pipe_nid_side_effect):
        with patch.object(
            fs, "_path_to_url_async", new_callable=AsyncMock, return_value="http://fake"
        ):
            with patch.object(fs, "_post", side_effect=fake_post):
                await fs._pipe_file(
                    "/file.txt",
                    b"content",
                    properties={"cm:title": "My File"},
                    aspects=["cm:titled", "cm:auditable"],
                )

    assert captured["properties"] == {"cm:title": "My File"}
    assert captured["aspectNames"] == ["cm:titled", "cm:auditable"]


async def test_pipe_file_without_metadata_omits_keys():
    """When no properties/aspects passed, POST data only has name and nodeType."""
    fs = make_fs()
    captured = {}

    async def fake_post(url, **kwargs):
        captured.update(kwargs.get("data", {}))
        return MagicMock(json=lambda: PIPE_POST_RESPONSE)

    with patch.object(fs, "_path_to_node_id", side_effect=_pipe_nid_side_effect):
        with patch.object(
            fs, "_path_to_url_async", new_callable=AsyncMock, return_value="http://fake"
        ):
            with patch.object(fs, "_post", side_effect=fake_post):
                await fs._pipe_file("/file.txt", b"content")

    assert "properties" not in captured
    assert "aspectNames" not in captured


async def test_pipe_file_update_ignores_metadata():
    """Properties/aspects are silently ignored when updating an existing node."""
    fs = make_fs()

    async def fake_put(url, **kwargs):
        return MagicMock()

    with patch.object(
        fs, "_path_to_node_id", new_callable=AsyncMock, return_value=NODE_ID
    ):
        with patch.object(
            fs, "_path_to_url_async", new_callable=AsyncMock, return_value="http://fake"
        ):
            with patch.object(fs, "_put", side_effect=fake_put) as mock_put:
                await fs._pipe_file(
                    "/file.txt",
                    b"new content",
                    properties={"cm:title": "My File"},
                    aspects=["cm:titled"],
                )

    # PUT to content URL is called; no POST (no node creation)
    mock_put.assert_called_once()


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


async def test_rm_file_skips_lookup_when_item_id_given():
    """item_id skips path→node_id lookup."""
    fs = make_fs()

    with patch.object(fs, "_path_to_node_id", new_callable=AsyncMock) as mock_nid:
        with patch.object(
            fs, "_path_to_url_async", new_callable=AsyncMock, return_value="http://fake"
        ):
            with patch.object(fs, "_delete", new_callable=AsyncMock):
                await fs._rm_file("/file.txt", item_id=NODE_ID)

    mock_nid.assert_not_called()


# ---------------------------------------------------------------------------
# Destructor
# ---------------------------------------------------------------------------


def test_destructor_calls_close_when_client_exists():
    """Must call close_http_session when _client is set."""
    fs = make_fs()
    fs._client = MagicMock()

    calls = []
    with patch.object(
        AlfrescoFS,
        "close_http_session",
        side_effect=lambda client, loop=None: calls.append(client),
    ):
        fs.__del__()

    assert len(calls) == 1


def test_destructor_does_nothing_when_client_is_none():
    """No-op if no client is set."""
    fs = make_fs()
    fs._client = None

    with patch.object(AlfrescoFS, "close_http_session") as mock_close:
        fs.__del__()

    mock_close.assert_not_called()


def test_destructor_swallows_exceptions():
    """Destructor must not raise even if closing fails."""
    fs = make_fs()
    fs._client = MagicMock()

    with patch.object(
        AlfrescoFS, "close_http_session", side_effect=RuntimeError("oops")
    ):
        fs.__del__()


# ---------------------------------------------------------------------------
# mkdir with properties / aspects
# ---------------------------------------------------------------------------

MKDIR_RESPONSE = {"entry": {"id": "new-folder-id", "name": "newfolder"}}


def _mkdir_nid_side_effect(path):
    """Return None for the new folder, ROOT_ID for its parent."""
    return ROOT_ID if path in ("/", "") else None


async def test_mkdir_returns_node_id():
    """_mkdir returns the created node ID."""
    fs = make_fs()

    with patch.object(fs, "_path_to_node_id", side_effect=_mkdir_nid_side_effect):
        with patch.object(
            fs,
            "_post",
            new_callable=AsyncMock,
            return_value=MagicMock(json=lambda: MKDIR_RESPONSE),
        ):
            nid = await fs._mkdir("/newfolder")

    assert nid == "new-folder-id"


async def test_mkdir_with_properties_and_aspects():
    """Properties and aspects are forwarded in the POST body."""
    fs = make_fs()
    captured = {}

    async def fake_post(url, **kwargs):
        captured.update(kwargs.get("json", {}))
        return MagicMock(json=lambda: MKDIR_RESPONSE)

    with patch.object(fs, "_path_to_node_id", side_effect=_mkdir_nid_side_effect):
        with patch.object(fs, "_post", side_effect=fake_post):
            await fs._mkdir(
                "/newfolder",
                properties={"cm:title": "My Folder"},
                aspects=["cm:titled", "cm:auditable"],
            )

    assert captured["properties"] == {"cm:title": "My Folder"}
    assert captured["aspectNames"] == [
        "cm:titled",
        "cm:auditable",
    ]


async def test_mkdir_without_properties_omits_keys():
    """When no properties/aspects passed, body only has name and nodeType."""
    fs = make_fs()
    captured = {}

    async def fake_post(url, **kwargs):
        captured.update(kwargs.get("json", {}))
        return MagicMock(json=lambda: MKDIR_RESPONSE)

    with patch.object(fs, "_path_to_node_id", side_effect=_mkdir_nid_side_effect):
        with patch.object(fs, "_post", side_effect=fake_post):
            await fs._mkdir("/newfolder")

    assert "properties" not in captured
    assert "aspectNames" not in captured


async def test_mkdir_exist_ok_returns_none():
    """exist_ok=True on 409 returns None instead of raising."""
    import httpx

    fs = make_fs()

    mock_response = MagicMock()
    mock_response.status_code = 409
    error = httpx.HTTPStatusError(
        "conflict", request=MagicMock(), response=mock_response
    )

    with patch.object(fs, "_path_to_node_id", side_effect=_mkdir_nid_side_effect):
        with patch.object(fs, "_post", new_callable=AsyncMock, side_effect=error):
            result = await fs._mkdir("/newfolder", exist_ok=True)

    assert result is None


# ---------------------------------------------------------------------------
# _update_metadata
# ---------------------------------------------------------------------------


async def test_update_metadata_sends_put():
    """PUT is called with properties and aspectNames in the body."""
    fs = make_fs()
    captured = {}

    async def fake_put(url, **kwargs):
        captured.update(kwargs.get("json", {}))

    with patch.object(fs, "_put", side_effect=fake_put):
        await fs._update_metadata(
            "",
            item_id=NODE_ID,
            properties={"cm:title": "Updated Title"},
            aspects=["cm:titled", "cm:auditable"],
        )

    assert captured["properties"] == {"cm:title": "Updated Title"}
    assert captured["aspectNames"] == ["cm:titled", "cm:auditable"]


async def test_update_metadata_only_properties():
    """AspectNames key is absent when only properties are passed."""
    fs = make_fs()
    captured = {}

    async def fake_put(url, **kwargs):
        captured.update(kwargs.get("json", {}))

    with patch.object(fs, "_put", side_effect=fake_put):
        await fs._update_metadata(
            "",
            item_id=NODE_ID,
            properties={"cm:title": "Updated Title"},
        )

    assert "properties" in captured
    assert "aspectNames" not in captured


async def test_update_metadata_only_aspects():
    """Properties key is absent when only aspects are passed."""
    fs = make_fs()
    captured = {}

    async def fake_put(url, **kwargs):
        captured.update(kwargs.get("json", {}))

    with patch.object(fs, "_put", side_effect=fake_put):
        await fs._update_metadata(
            "",
            item_id=NODE_ID,
            aspects=["cm:titled"],
        )

    assert "aspectNames" in captured
    assert "properties" not in captured


async def test_update_metadata_noop_when_empty():
    """No HTTP call is made when neither properties nor aspects are given."""
    fs = make_fs()

    with patch.object(fs, "_put", new_callable=AsyncMock) as mock_put:
        await fs._update_metadata("", item_id=NODE_ID)

    mock_put.assert_not_called()


async def test_update_metadata_resolves_path_when_no_item_id():
    """When item_id is not given, path is resolved to a node ID."""
    fs = make_fs()
    captured = {}

    async def fake_put(url, **kwargs):
        captured.update(kwargs.get("json", {}))

    with patch.object(
        fs, "_path_to_node_id", new_callable=AsyncMock, return_value=NODE_ID
    ) as mock_nid:
        with patch.object(fs, "_put", side_effect=fake_put):
            await fs._update_metadata(
                "/some/path",
                properties={"cm:title": "Updated Title"},
            )

    mock_nid.assert_called_once_with("/some/path")
    assert captured["properties"] == {"cm:title": "Updated Title"}


# ---------------------------------------------------------------------------
# Node → fsspec info mapping
# ---------------------------------------------------------------------------


def _file_entry(node_id=NODE_ID):
    """Fake file node entry."""
    return {
        "id": node_id,
        "name": "file.txt",
        "isFolder": False,
        "nodeType": "cm:content",
        "createdAt": "2024-01-01T00:00:00.000+0000",
        "modifiedAt": "2024-01-01T00:00:00.000+0000",
        "parentId": ROOT_ID,
        "content": {"sizeInBytes": 10, "mimeType": "text/plain"},
        "path": {"elements": [{"id": ROOT_ID, "name": "Company Home"}]},
    }


def _dir_entry():
    """Fake directory node entry."""
    return {
        "id": "dir-id",
        "name": "folder",
        "isFolder": True,
        "nodeType": "cm:folder",
        "createdAt": "2024-01-01T00:00:00.000+0000",
        "modifiedAt": "2024-01-01T00:00:00.000+0000",
        "parentId": ROOT_ID,
        "path": {"elements": [{"id": ROOT_ID, "name": "Company Home"}]},
    }


def test_weburl_present_when_content_app_url_set():
    """File entries include web URL when content app URL is configured."""
    fs = make_fs(content_app_url=CONTENT_APP_URL)

    info = fs._node_entry_to_fsspec_info(_file_entry())

    assert "weburl" in info
    assert NODE_ID in info["weburl"]
    assert info["weburl"].startswith(CONTENT_APP_URL)


def test_weburl_absent_without_content_app_url():
    """No web URL if content app URL is not configured."""
    fs = make_fs()

    info = fs._node_entry_to_fsspec_info(_file_entry())

    assert "weburl" not in info


def test_directory_has_no_weburl():
    """Directories never have a web URL."""
    fs = make_fs(content_app_url=CONTENT_APP_URL)

    info = fs._node_entry_to_fsspec_info(_dir_entry())

    assert "weburl" not in info
