import pytest

# ---------------------------------------------------------------------------
# touch
# ---------------------------------------------------------------------------


def test_touch(temp_fs):
    """Creates a file that did not previously exist."""
    assert not temp_fs.exists("/newfile")
    temp_fs.touch("/newfile")
    assert temp_fs.exists("/newfile")


# ---------------------------------------------------------------------------
# rm / rm_file / rmdir
# ---------------------------------------------------------------------------


def test_rm(temp_nested_fs):
    """Removes a single file, a list of files, and a directory recursively."""
    fs = temp_nested_fs
    fs.rm("/emptyfile")
    assert not fs.exists("/emptyfile")
    fs.rm(["/nested/file1", "/nested/file2"])
    assert not fs.exists("/nested/file1")
    fs.rm("/nested", recursive=True)
    assert not fs.exists("/nested")


def test_rm_file(temp_fs):
    """Removes a single file."""
    temp_fs.touch("/file1")
    temp_fs.rm_file("/file1")
    assert not temp_fs.exists("/file1")


def test_rmdir(temp_nested_fs):
    """Raises on non-directory or non-empty directory; succeeds when empty."""
    fs = temp_nested_fs
    with pytest.raises(FileNotFoundError, match=r"Directory not found: .*\/emptyfile"):
        fs.rmdir("/emptyfile")
    with pytest.raises(OSError, match=r"Directory not empty: .*\/nested"):
        fs.rmdir("/nested/nested2")
    with pytest.raises(FileNotFoundError):
        fs.rmdir("/unknown")
    fs.rm("/nested/nested2/file1")
    fs.rm("/nested/nested2/file2")
    fs.rmdir("/nested/nested2")
    assert not fs.exists("/nested/nested2")


# ---------------------------------------------------------------------------
# mkdir / makedirs
# ---------------------------------------------------------------------------


def test_mkdir(temp_fs):
    """Creates a directory; respects create_parents flag."""
    fs = temp_fs
    fs.mkdir("/newdir")
    assert fs.exists("/newdir")
    assert "/newdir" in fs.ls("/", detail=False)
    with pytest.raises(
        FileNotFoundError,
        match=r"Parent directory does not exist: .*\/newdir\/nested",
    ):
        fs.mkdir("/newdir/nested/subnested", create_parents=False)
    fs.mkdir("/newdir/nested/subnested", create_parents=True)
    assert fs.exists("/newdir/nested/subnested")


def test_makedirs(temp_fs):
    """Creates a full path including parents; raises if already exists."""
    fs = temp_fs
    fs.makedirs("/newdir/nested/subnested")
    assert fs.exists("/newdir/nested/subnested")
    with pytest.raises(FileExistsError, match=r"Directory already exists: .*\/newdir"):
        fs.makedirs("/newdir")


# ---------------------------------------------------------------------------
# copy / move
# ---------------------------------------------------------------------------


def test_copy(temp_fs):
    """Copies a file and a directory tree recursively."""
    fs = temp_fs
    fs.pipe_file("/file1.txt", b"hello world")
    fs.copy("/file1.txt", "/file2.txt")
    assert fs.cat("/file2.txt") == b"hello world"
    fs.makedirs("/origin/nested")
    fs.touch("/origin/nested/child.txt")
    fs.copy("/origin", "/destination", recursive=True)
    assert fs.exists("/destination/nested/child.txt")


def test_move(temp_fs):
    """Renames a file and moves it into a directory."""
    fs = temp_fs
    fs.pipe_file("/file1.txt", b"hello world")
    fs.move("/file1.txt", "/file2.txt")
    assert not fs.exists("/file1.txt")
    assert fs.cat("/file2.txt") == b"hello world"
    fs.makedirs("/origin/nested")
    fs.move("/file2.txt", "/origin/nested/")
    assert not fs.exists("/file2.txt")
    assert fs.cat("/origin/nested/file2.txt") == b"hello world"
