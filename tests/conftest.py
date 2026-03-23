import os
import uuid
from contextlib import asynccontextmanager, contextmanager

import pytest
import pytest_asyncio
from fsspec.implementations.dirfs import DirFileSystem

from alfrescofs import AlfrescoFS


@pytest.fixture(scope="session")
def test_files():
    """Sample JSON files with simple records."""
    return {
        "test/accounts.1.json": (
            b'{"amount": 100, "name": "Alice"}\n'
            b'{"amount": 200, "name": "Bob"}\n'
            b'{"amount": 300, "name": "Charlie"}\n'
            b'{"amount": 400, "name": "Dennis"}\n'
        ),
        "test/accounts.2.json": (
            b'{"amount": 500, "name": "Alice"}\n'
            b'{"amount": 600, "name": "Bob"}\n'
            b'{"amount": 700, "name": "Charlie"}\n'
            b'{"amount": 800, "name": "Dennis"}\n'
        ),
    }


@pytest.fixture(scope="session")
def test_csv_files():
    """Sample CSV files, including an empty one."""
    return {
        "csv/2014-01-01.csv": b"name,amount,id\nAlice,100,1\nBob,200,2\nCharlie,300,3\n",
        "csv/2014-01-02.csv": b"name,amount,id\n",
    }


@pytest.fixture(scope="session")
def test_text_files():
    """Nested text files for directory structure tests."""
    return {
        "nested/file1": b"hello\n",
        "nested/file2": b"world",
        "nested/nested2/file1": b"hello\n",
        "nested/nested2/file2": b"world",
    }


@pytest.fixture(scope="session")
def test_glob_files():
    """Files used to test glob pattern matching."""
    return {"file.dat": b"", "filexdat": b""}


@pytest.fixture(scope="session")
def all_test_data(test_files, test_csv_files, test_text_files, test_glob_files):
    """All test datasets grouped together."""
    return {
        "files": test_files,
        "csv_files": test_csv_files,
        "text_files": test_text_files,
        "glob_files": test_glob_files,
    }


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add CLI options for Alfresco connection settings."""
    parser.addoption(
        "--base-url", action="store", default=None, help="Alfresco base URL"
    )
    parser.addoption(
        "--username", action="store", default=None, help="Alfresco username"
    )
    parser.addoption(
        "--password", action="store", default=None, help="Alfresco password"
    )


def _create_fs(request, asynchronous: bool = False) -> AlfrescoFS:
    """Create an AlfrescoFS instance.

    Reads credentials from CLI options or environment variables. Skips
    tests if required values are missing.
    """
    base_url = request.config.getoption("--base-url") or os.getenv(
        "ALFRESCOFS_BASE_URL"
    )
    username = request.config.getoption("--username") or os.getenv(
        "ALFRESCOFS_USERNAME"
    )
    password = request.config.getoption("--password") or os.getenv(
        "ALFRESCOFS_PASSWORD"
    )

    if not base_url or not username or not password:
        pytest.skip(
            "Skipping integration test — missing credentials. "
            "Provide --base-url, --username, --password "
            "or the ALFRESCOFS_BASE_URL / ALFRESCOFS_USERNAME / ALFRESCOFS_PASSWORD env vars."
        )

    AlfrescoFS.clear_instance_cache()
    return AlfrescoFS(
        base_url=base_url,
        auth_type="basic",
        username=username,
        password=password,
        asynchronous=asynchronous,
    )


FS_TYPES = ["alfresco"]


@pytest.fixture(scope="module", params=FS_TYPES)
def fs(request):
    """Synchronous Alfresco filesystem."""
    yield _create_fs(request, asynchronous=False)


@pytest.fixture(scope="module", params=FS_TYPES)
def afs(request):
    """Asynchronous Alfresco filesystem."""
    yield _create_fs(request, asynchronous=True)


class AlfrescoTempFS(DirFileSystem):
    """Filesystem wrapper scoped to a temporary base path.

    All paths are kept relative to a generated temp directory.
    """

    def _relpath(self, path):
        """Re-add the leading '/' stripped by DirFileSystem."""
        path = super()._relpath(path)
        if isinstance(path, str) and not path.startswith("/"):
            path = "/" + path
        return path

    def _join(self, path):
        """Normalize path before passing to underlying FS."""
        if isinstance(path, str) and path.startswith("/"):
            path = path[1:]
        return super()._join(path)

    async def _rmdir(self, path):
        """Remove directory (async)."""
        return await self.fs._rmdir(self._join(path))

    async def _touch(self, path, **kwargs):
        """Create empty file (async)."""
        return await self.fs._touch(self._join(path), **kwargs)

    async def _move(self, path1, path2, **kwargs):
        """Move or rename file (async)."""
        return await self.fs._mv_file(self._join(path1), self._join(path2), **kwargs)

    async def _open_async(self, path, mode, **kwargs):
        """Open file asynchronously."""
        return await self.fs.open_async(self._join(path), mode, **kwargs)

    async def _modified(self, path):
        """Get last modified timestamp (async)."""
        return await self.fs._modified(self._join(path))

    async def _created(self, path):
        """Get creation timestamp (async)."""
        return await self.fs._created(self._join(path))

    async def _checkout(self, path, item_id=None):
        """Checkout file (async)."""
        return await self.fs._checkout(self._join(path), item_id)

    def checkout(self, path, item_id=None):
        """Checkout file."""
        return self.fs.checkout(self._join(path), item_id)

    async def _checkin(self, path, comment=None, item_id=None):
        """Checkin file (async)."""
        return await self.fs._checkin(self._join(path), comment, item_id)

    def checkin(self, path, comment=None, item_id=None):
        """Checkin file."""
        return self.fs.checkin(self._join(path), comment, item_id)

    async def _get_versions(self, path):
        """Get version history (async)."""
        return await self.fs._get_versions(self._join(path))

    def get_versions(self, path):
        """Get version history."""
        return self.fs.get_versions(self._join(path))

    async def _get_permissions(self, path, item_id=None):
        """Get permissions (async)."""
        return await self.fs._get_permissions(self._join(path), item_id)

    def get_permissions(self, path, item_id=None):
        """Get permissions."""
        return self.fs.get_permissions(self._join(path), item_id)


@contextmanager
def _temp_dir(storagefs):
    """Create a temporary directory and remove it after use."""
    temp_dir_name = f"/{str(uuid.uuid4())}"
    storagefs.mkdir(temp_dir_name)
    try:
        yield temp_dir_name
    finally:
        storagefs.rm(temp_dir_name, recursive=True)


@asynccontextmanager
async def _a_temp_dir(storagefs):
    """Async version of temporary directory helper."""
    temp_dir_name = f"/{str(uuid.uuid4())}"
    await storagefs._mkdir(temp_dir_name)
    try:
        yield temp_dir_name
    finally:
        await storagefs._rm(temp_dir_name, recursive=True)


@pytest.fixture(scope="module")
def sample_fs(fs, all_test_data):
    """Filesystem preloaded with sample files (sync)."""
    with _temp_dir(fs) as temp_dir_name:
        sfs = AlfrescoTempFS(path=temp_dir_name, fs=fs)
        for flist in all_test_data.values():
            for path, data in flist.items():
                root, _ = os.path.split(path)
                if root:
                    sfs.makedirs(root, exist_ok=True)
                with sfs.open(path, "wb") as f:
                    f.write(data)
        sfs.makedirs("/emptydir")
        yield sfs


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def sample_afs(afs, all_test_data):
    """Filesystem preloaded with sample files (async)."""
    async with _a_temp_dir(afs) as temp_dir_name:
        sfs = AlfrescoTempFS(path=temp_dir_name, asynchronous=True, fs=afs)
        for flist in all_test_data.values():
            for path, data in flist.items():
                root, _ = os.path.split(path)
                if root:
                    await sfs._makedirs(root, exist_ok=True)
                async with await sfs._open_async(path, "wb") as f:
                    await f.write(data)
        await sfs._makedirs("/emptydir")
        yield sfs


@pytest.fixture(scope="function")
def temp_fs(fs):
    """Empty temporary filesystem per test (sync)."""
    with _temp_dir(fs) as temp_dir_name:
        yield AlfrescoTempFS(path=temp_dir_name, fs=fs)


@pytest_asyncio.fixture(scope="function", params=FS_TYPES, loop_scope="function")
async def function_afs(request):
    """Fresh async filesystem per test."""
    yield _create_fs(request, asynchronous=True)


@pytest_asyncio.fixture(scope="function", loop_scope="function")
async def temp_afs(function_afs):
    """Empty temporary filesystem per test (async)."""
    async with _a_temp_dir(function_afs) as temp_dir_name:
        yield AlfrescoTempFS(path=temp_dir_name, asynchronous=True, fs=function_afs)


@pytest.fixture(scope="function")
def temp_nested_fs(fs, test_text_files):
    """Temporary filesystem with nested files (sync)."""
    with _temp_dir(fs) as temp_dir_name:
        sfs = AlfrescoTempFS(path=temp_dir_name, fs=fs)
        for path, data in test_text_files.items():
            root, _ = os.path.split(path)
            if root:
                sfs.makedirs(root, exist_ok=True)
            with sfs.open(path, "wb") as f:
                f.write(data)
        sfs.touch("/emptyfile")
        yield sfs


@pytest_asyncio.fixture(scope="function", loop_scope="function")
async def temp_nested_afs(function_afs, test_text_files):
    """Temporary filesystem with nested files (async)."""
    async with _a_temp_dir(function_afs) as temp_dir_name:
        sfs = AlfrescoTempFS(path=temp_dir_name, asynchronous=True, fs=function_afs)
        for path, data in test_text_files.items():
            root, _ = os.path.split(path)
            if root:
                await sfs._makedirs(root, exist_ok=True)
            async with await sfs._open_async(path, "wb") as f:
                await f.write(data)
        await sfs.fs._touch(sfs._join("/emptyfile"))
        yield sfs
