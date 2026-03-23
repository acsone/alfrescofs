import pytest

# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------


def test_write_small(temp_fs):
    """Writes a small payload in a single call (one-shot pipe_file path)."""
    with temp_fs.open("/test.csv", "wb") as f:
        f.write(b"hello world")
    assert temp_fs.cat("/test.csv") == b"hello world"


@pytest.mark.asyncio(loop_scope="function")
async def test_async_write_small(temp_afs):
    """Same as test_write_small via open_async() → AlfrescoStreamedFile."""
    async with await temp_afs._open_async("/test.csv", "wb") as f:
        await f.write(b"hello world")
    assert await temp_afs._cat("/test.csv") == b"hello world"


def test_write_large(temp_fs):
    """Writes 1 MB in a single call with 320 KB block_size, forcing chunked upload."""
    payload = b"0" * 2**20
    block_size = 320 * 2**10
    with temp_fs.open("/test.bin", "wb", block_size=block_size) as f:
        f.write(payload)
    assert temp_fs.cat("/test.bin") == payload


@pytest.mark.asyncio(loop_scope="function")
async def test_async_write_large(temp_afs):
    """Same as test_write_large via open_async() → AlfrescoStreamedFile."""
    payload = b"0" * 2**20
    block_size = 320 * 2**10
    async with await temp_afs._open_async(
        "/test.bin", "wb", block_size=block_size
    ) as f:
        await f.write(payload)
    assert await temp_afs._cat("/test.bin") == payload


def test_write_blocks(temp_fs):
    """Writes 1 MB in 50 KB chunks (not divisible by block_size), exercising chunk
    boundaries."""
    payload = b"0" * 2**20
    block_size = 320 * 2**10
    chunk_size = 50_000
    with temp_fs.open("/test.bin", "wb", block_size=block_size) as f:
        for i in range(0, len(payload), chunk_size):
            f.write(payload[i : i + chunk_size])
    assert temp_fs.cat("/test.bin") == payload


def test_open_no_write(temp_fs):
    """Opening in wb and closing without writing creates an empty file."""
    with temp_fs.open("/test.csv", "wb") as f:
        assert f.tell() == 0
    assert temp_fs.cat("/test.csv") == b""


@pytest.mark.asyncio(loop_scope="function")
async def test_async_open_no_write(temp_afs):
    """Same as test_open_no_write via open_async() → AlfrescoStreamedFile."""
    async with await temp_afs._open_async("/test.csv", "wb") as f:
        assert f.tell() == 0
    assert await temp_afs._cat("/test.csv") == b""


# ---------------------------------------------------------------------------
# cat_ranges
# ---------------------------------------------------------------------------


def test_cat_ranges(temp_fs):
    """cat_file respects start, end, and negative-offset slice semantics."""
    data = b"a string to select from"
    temp_fs.pipe("/parts", data)
    assert temp_fs.cat_file("/parts", start=5) == data[5:]
    assert temp_fs.cat_file("/parts", end=5) == data[:5]
    assert temp_fs.cat_file("/parts", start=1, end=-1) == data[1:-1]
    assert temp_fs.cat_file("/parts", start=-5) == data[-5:]
