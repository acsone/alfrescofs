import pytest

# ---------------------------------------------------------------------------
# Listing / navigation
# ---------------------------------------------------------------------------


def test_ls(sample_fs):
    """Lists directory contents correctly."""
    result = sample_fs.ls("/", detail=False)

    assert sorted(result) == sorted(
        ["/csv", "/emptydir", "/file.dat", "/filexdat", "/nested", "/test"]
    )

    assert sorted(sample_fs.ls("/test", detail=False)) == [
        "/test/accounts.1.json",
        "/test/accounts.2.json",
    ]

    assert sample_fs.ls("/emptydir", detail=False) == []


def test_glob(sample_fs):
    """Glob patterns return matching files."""
    result = sample_fs.glob("/*/*.json")

    assert set(result) == {"/test/accounts.1.json", "/test/accounts.2.json"}


# ---------------------------------------------------------------------------
# File metadata
# ---------------------------------------------------------------------------


def test_info(sample_fs, test_files):
    """Returns correct metadata for a file."""
    import datetime

    info = sample_fs.info("/test/accounts.1.json")

    assert info["type"] == "file"
    assert info["size"] == len(test_files["test/accounts.1.json"])
    assert info["mimetype"] == "application/json"

    assert isinstance(info["mtime"], datetime.datetime)


def test_isdir_isfile(sample_fs):
    """Correctly distinguishes files and directories."""
    assert sample_fs.isdir("/test")
    assert not sample_fs.isfile("/test")

    assert sample_fs.isfile("/test/accounts.1.json")
    assert not sample_fs.isdir("/test/accounts.1.json")


# ---------------------------------------------------------------------------
# File content
# ---------------------------------------------------------------------------


def test_cat(sample_fs, test_files):
    """Reads full file content."""
    for path, data in test_files.items():
        assert sample_fs.cat(f"/{path}") == data


def test_seek_read(sample_fs):
    """Supports seek and partial reads (sync)."""
    with sample_fs.open("/nested/file2", "rb") as f:
        f.seek(1)
        assert f.read(1) == b"o"

        f.seek(-1, 2)
        assert f.read(1) == b"d"


@pytest.mark.asyncio(loop_scope="module")
async def test_async_seek_read(sample_afs):
    """Supports seek and partial reads (async)."""
    async with await sample_afs._open_async("/nested/file2", "rb") as f:
        f.seek(1)
        assert await f.read(1) == b"o"

        f.seek(-1, 2)
        assert await f.read(1) == b"d"
