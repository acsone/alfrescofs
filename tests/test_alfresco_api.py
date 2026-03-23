from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alfrescofs import AlfrescoFS

BASE_URL = "http://mock-api.test:8080"
ROOT_ID = "root-api-id"
NODE_ID = "some-node-id"

PERMISSIONS_DICT = {
    "inherited": [
        {"authorityId": "GROUP_EVERYONE", "name": "Consumer", "accessStatus": "ALLOWED"}
    ],
    "locallySet": [
        {"authorityId": "admin", "name": "FullControl", "accessStatus": "ALLOWED"}
    ],
}

TRASH_ENTRY_1 = {"id": "deleted-1", "name": "old-file.txt", "nodeType": "cm:content"}
TRASH_ENTRY_2 = {"id": "deleted-2", "name": "old-folder", "nodeType": "cm:folder"}


def make_fs() -> AlfrescoFS:
    """Create a fresh AlfrescoFS instance with mocked base settings."""
    AlfrescoFS.clear_instance_cache()
    fs = AlfrescoFS(base_url=BASE_URL, auth_type="basic", username="u", password="p")
    fs._root_node_id = ROOT_ID
    return fs


def trash_payload(entries, has_more=False, skip=0):
    """Build a fake API response for trash listing (with pagination support)."""
    return {
        "list": {
            "entries": [{"entry": e} for e in entries],
            "pagination": {
                "hasMoreItems": has_more,
                "skipCount": skip,
                "count": len(entries),
            },
        }
    }


# ---------------------------------------------------------------------------
# Permissions
# ---------------------------------------------------------------------------


async def test_get_permissions_returns_dict():
    """Returns permissions from node info."""
    fs = make_fs()
    info = {"item_info": {"id": NODE_ID, "permissions": PERMISSIONS_DICT}}

    with patch.object(fs, "_info", new_callable=AsyncMock, return_value=info):
        result = await fs._get_permissions("/file.txt")

    assert result == PERMISSIONS_DICT


async def test_get_permissions_calls_info_with_include():
    """Must call _info with include='permissions'."""
    fs = make_fs()
    info = {"item_info": {"id": NODE_ID, "permissions": PERMISSIONS_DICT}}

    with patch.object(
        fs, "_info", new_callable=AsyncMock, return_value=info
    ) as mock_info:
        await fs._get_permissions("/file.txt")

    mock_info.assert_called_once_with("/file.txt", item_id=None, include="permissions")


async def test_get_permissions_returns_empty_when_absent():
    """Returns empty dict if permissions are missing."""
    fs = make_fs()

    with patch.object(
        fs, "_info", new_callable=AsyncMock, return_value={"item_info": {"id": NODE_ID}}
    ):
        result = await fs._get_permissions("/file.txt")

    assert result == {}


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


async def test_list_deleted_nodes_returns_entries():
    """Lists deleted nodes from trash."""
    fs = make_fs()
    captured_urls = []

    async def fake_get(url, **kwargs):
        captured_urls.append(str(url))
        return trash_payload([TRASH_ENTRY_1, TRASH_ENTRY_2])

    with patch.object(fs, "_get_json", side_effect=fake_get):
        result = await fs._list_deleted_nodes()

    assert len(result) == 2
    assert result[0]["id"] == "deleted-1"
    assert "deleted-nodes" in captured_urls[0]


async def test_list_deleted_nodes_pagination():
    """Handles paginated trash responses."""
    fs = make_fs()

    mock_get = AsyncMock(
        side_effect=[
            trash_payload([TRASH_ENTRY_1], has_more=True),
            trash_payload([TRASH_ENTRY_2], has_more=False),
        ]
    )

    with patch.object(fs, "_get_json", mock_get):
        result = await fs._list_deleted_nodes()

    assert len(result) == 2
    assert mock_get.call_count == 2


async def test_restore_deleted_node_calls_endpoint():
    """Calls restore endpoint for a deleted node."""
    fs = make_fs()

    mock_resp = MagicMock()
    mock_resp.json.return_value = {"entry": {"id": "restored-1"}}

    with patch.object(
        fs, "_post", new_callable=AsyncMock, return_value=mock_resp
    ) as mock_post:
        await fs._restore_deleted_node(NODE_ID)

    url_arg = str(mock_post.call_args[0][0])
    assert f"deleted-nodes/{NODE_ID}/restore" in url_arg


async def test_purge_deleted_node_calls_endpoint():
    """Calls delete on the correct endpoint, not /restore."""
    fs = make_fs()

    with patch.object(fs, "_delete", new_callable=AsyncMock) as mock_delete:
        await fs._purge_deleted_node(NODE_ID)

    url_arg = str(mock_delete.call_args[0][0])
    assert f"deleted-nodes/{NODE_ID}" in url_arg
    assert "restore" not in url_arg


# ---------------------------------------------------------------------------
# Renditions
# ---------------------------------------------------------------------------


async def test_request_rendition_raises_if_not_file():
    """Fails if rendition is requested for a non-file path."""
    fs = make_fs()

    with patch.object(fs, "_isfile", new_callable=AsyncMock, return_value=False):
        with pytest.raises(FileNotFoundError):
            await fs._request_rendition("/dir")


async def test_request_rendition_custom_id():
    """Sends custom rendition ID in request payload."""
    fs = make_fs()

    with patch.object(fs, "_isfile", new_callable=AsyncMock, return_value=True):
        with patch.object(
            fs, "_path_to_url_async", new_callable=AsyncMock, return_value="http://fake"
        ):
            with patch.object(
                fs, "_post", new_callable=AsyncMock, return_value=MagicMock()
            ) as mock_post:
                await fs._request_rendition("/file.docx", rendition_id="imgpreview")

    assert mock_post.call_args[1]["json"] == {"id": "imgpreview"}


async def test_download_rendition_content_raises_if_not_file():
    """Fails if trying to fetch rendition for a non-file."""
    fs = make_fs()

    with patch.object(fs, "_isfile", new_callable=AsyncMock, return_value=False):
        with pytest.raises(FileNotFoundError):
            await fs._download_rendition_content("/dir")


async def test_download_rendition_content():
    """Fetches binary content of a rendition."""
    fs = make_fs()

    expected = b"%PDF-1.4 fake content"
    mock_resp = MagicMock()
    mock_resp.content = expected

    with patch.object(fs, "_isfile", new_callable=AsyncMock, return_value=True):
        with patch.object(
            fs, "_path_to_url_async", new_callable=AsyncMock, return_value="http://fake"
        ) as mock_url:
            with patch.object(
                fs, "_get", new_callable=AsyncMock, return_value=mock_resp
            ):
                result = await fs._download_rendition_content("/file.docx")

    mock_url.assert_called_once_with(
        path="/file.docx", node_id=None, parts=("renditions", "pdf", "content")
    )
    assert result == expected
