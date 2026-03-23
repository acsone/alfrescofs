import pytest


def test_append(temp_nested_fs, all_test_data):
    """Appends to small and large files, with and without actual writes."""
    fs = temp_nested_fs
    data = all_test_data["text_files"]["nested/file1"]

    # Small file, no write: content unchanged, tell() == 0 (lazy load)
    with fs.open("/nested/file1", "ab") as f:
        assert f.tell() == 0
    assert fs.cat("/nested/file1") == data

    # Small file, write: content extended
    with fs.open("/nested/file1", "ab") as f:
        f.write(b"extra")
    assert fs.cat("/nested/file1") == data + b"extra"

    block_size = 320 * 2**10
    big = b"a" * 2**20
    fs.pipe_file("/bigfile", big)

    # Large file, no write: content unchanged
    with fs.open("/bigfile", "ab", block_size=block_size) as f:
        pass
    assert fs.cat("/bigfile") == big

    # Large file, small append: tell() reflects full size after lazy load
    with fs.open("/bigfile", "ab", block_size=block_size) as f:
        f.write(b"extra")
        assert f.tell() == len(big) + 5
    assert fs.cat("/bigfile") == big + b"extra"

    # Large file, large append
    big2 = b"b" * 2**20
    with fs.open("/bigfile", "ab", block_size=block_size) as f:
        f.write(big2)
        assert f.tell() == len(big) + 5 + len(big2)
    assert fs.cat("/bigfile") == big + b"extra" + big2


@pytest.mark.asyncio(loop_scope="function")
async def test_async_append(temp_nested_afs, all_test_data):
    """Same as test_append via open_async() → AlfrescoStreamedFile."""
    fs = temp_nested_afs
    data = all_test_data["text_files"]["nested/file1"]

    async with await fs._open_async("/nested/file1", "ab") as f:
        assert f.tell() == 0
    assert await fs._cat("/nested/file1") == data

    async with await fs._open_async("/nested/file1", "ab") as f:
        await f.write(b"extra")
    assert await fs._cat("/nested/file1") == data + b"extra"

    block_size = 320 * 2**10
    big = b"a" * 2**20
    await fs._pipe_file("/bigfile", big)

    async with await fs._open_async("/bigfile", "ab", block_size=block_size) as f:
        pass
    assert await fs._cat("/bigfile") == big

    async with await fs._open_async("/bigfile", "ab", block_size=block_size) as f:
        await f.write(b"extra")
        assert f.tell() == len(big) + 5
    assert await fs._cat("/bigfile") == big + b"extra"

    big2 = b"b" * 2**20
    async with await fs._open_async("/bigfile", "ab", block_size=block_size) as f:
        await f.write(big2)
        assert f.tell() == len(big) + 5 + len(big2)
    assert await fs._cat("/bigfile") == big + b"extra" + big2
