import datetime

# ---------------------------------------------------------------------------
# timestamps
# ---------------------------------------------------------------------------


def test_timestamps(temp_fs):
    """Modified() and created() return datetime instances."""
    temp_fs.touch("/test.csv")
    assert isinstance(temp_fs.modified("/test.csv"), datetime.datetime)
    assert isinstance(temp_fs.created("/test.csv"), datetime.datetime)


# ---------------------------------------------------------------------------
# checkout / checkin (lock / unlock)
# ---------------------------------------------------------------------------


def test_lock_unlock(temp_fs):
    """Checkout() + checkin() creates a new version entry."""
    fs = temp_fs
    fs.touch("/test.csv")
    assert len(fs.get_versions("/test.csv")) == 1
    fs.checkout("/test.csv")
    fs.checkin("/test.csv", comment="my update comment")
    assert len(fs.get_versions("/test.csv")) == 2


# ---------------------------------------------------------------------------
# versions
# ---------------------------------------------------------------------------


def test_versions(temp_fs):
    """Each write increments the version count; latest version is flagged."""
    fs = temp_fs
    fs.pipe_file("/versioned.txt", b"v1 content")
    v1 = fs.get_versions("/versioned.txt")
    assert len(v1) >= 1
    fs.pipe_file("/versioned.txt", b"v2 content")
    v2 = fs.get_versions("/versioned.txt")
    assert len(v2) > len(v1)
    assert any(v.get("isLatestVersion") for v in v2)
