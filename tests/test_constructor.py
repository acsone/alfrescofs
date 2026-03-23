import os
from unittest.mock import patch

import fsspec
import httpx
import pytest
from authlib.integrations.httpx_client import AsyncOAuth2Client

from alfrescofs import AlfrescoFS

BASE_URL = "http://alfresco.example.com:8080"


@pytest.fixture(autouse=True)
def clear_cache():
    """Ensure no shared FS instances between tests."""
    AlfrescoFS.clear_instance_cache()


def make_fs(**kwargs):
    """Create a basic-auth AlfrescoFS with defaults."""
    return AlfrescoFS(
        base_url=BASE_URL, auth_type="basic", username="u", password="p", **kwargs
    )


# ---------------------------------------------------------------------------
# fsspec integration
# ---------------------------------------------------------------------------


def test_fsspec_registration():
    """Filesystem is registered under 'alfd' protocol."""
    assert "alfd" in fsspec.available_protocols()

    fs = fsspec.filesystem("alfd", base_url=BASE_URL, username="u", password="p")
    assert isinstance(fs, AlfrescoFS)


# ---------------------------------------------------------------------------
# Constructor behavior
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "param, value, attr, expected",
    [
        ("api_path", "api/v2", "_api_root", "v2"),
        ("root_path", "Sites/mysite/", "_root_path", "/Sites/mysite"),
    ],
)
def test_constructor_params(param, value, attr, expected):
    """Constructor normalizes selected parameters."""
    fs = make_fs(**{param: value})
    assert expected in str(getattr(fs, attr))


@pytest.mark.parametrize(
    "env, kwargs, expected_attr, expected_val",
    [
        (
            {"ALFRESCOFS_BASE_URL": "http://env-host"},
            {"username": "u", "password": "p"},
            "_base_url",
            "env-host",
        ),
        (
            {"ALFRESCOFS_USERNAME": "env-u", "ALFRESCOFS_PASSWORD": "env-p"},
            {"base_url": BASE_URL},
            "_username",
            "env-u",
        ),
        (
            {"ALFRESCOFS_CONTENT_APP_URL": "http://content"},
            {"base_url": BASE_URL, "username": "u", "password": "p"},
            "_content_app_url",
            "http://content",
        ),
    ],
)
def test_env_vars(env, kwargs, expected_attr, expected_val):
    """Environment variables override or fill missing constructor values."""
    with patch.dict(os.environ, env, clear=True):
        fs = AlfrescoFS(**kwargs)
        assert expected_val in str(getattr(fs, expected_attr))


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


def test_auth_clients_basic():
    """Basic auth requires username/password and returns httpx client."""
    assert isinstance(make_fs()._get_client(), httpx.AsyncClient)

    with pytest.raises(ValueError, match="username and password"):
        AlfrescoFS(base_url=BASE_URL, auth_type="basic")._get_client()


def test_auth_clients_oauth2():
    """OAuth2 uses AsyncOAuth2Client and validates required params."""
    params = {"client_id": "id", "client_secret": "s", "token_endpoint": "http://t/"}
    fs = AlfrescoFS(base_url=BASE_URL, auth_type="oauth2", oauth2_client_params=params)

    assert isinstance(fs._get_client(), AsyncOAuth2Client)

    with pytest.raises(ValueError, match="oauth2_client_params"):
        AlfrescoFS(base_url=BASE_URL, auth_type="oauth2")._get_client()


def test_oauth2_env_logic():
    """OAuth2 params are completed from env, but explicit values take precedence."""
    env = {
        "ALFRESCOFS_CLIENT_ID": "env-id",
        "ALFRESCOFS_TOKEN_ENDPOINT": "http://env-t/",
    }

    with patch.dict(os.environ, env):
        # Env fills missing values
        fs = AlfrescoFS(
            base_url=BASE_URL,
            auth_type="oauth2",
            oauth2_client_params={"client_secret": "s"},
        )
        assert fs._oauth2_params["client_id"] == "env-id"
        assert fs._oauth2_params["token_endpoint"] == "http://env-t/"

        # Explicit overrides env
        fs = AlfrescoFS(
            base_url=BASE_URL,
            auth_type="oauth2",
            oauth2_client_params={
                "client_id": "exp",
                "client_secret": "s",
                "token_endpoint": "http://t/",
            },
        )
        assert fs._oauth2_params["client_id"] == "exp"
