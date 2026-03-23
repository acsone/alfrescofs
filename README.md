# alfrescofs

An [fsspec](https://filesystem-spec.readthedocs.io/) adapter for [Alfresco Content Services](https://www.alfresco.com/).

It lets you read and write files in Alfresco using the same interface as any other filesystem in the Python ecosystem (local disk, S3, etc.).

## Installation

```bash
pip install alfrescofs
```

## Quickstart

```python
import fsspec

fs = fsspec.filesystem(
    "alfd",
    base_url="https://your-alfresco-host",
    username="admin",
    password="admin",
)

# List files
fs.ls("/Sites/mysite/documentLibrary")

# Read a file
with fs.open("/Sites/mysite/documentLibrary/report.csv") as f:
    content = f.read()

# Write a file
with fs.open("/Sites/mysite/documentLibrary/output.txt", "wb") as f:
    f.write(b"hello world")
```

You can also use it directly with pandas or any other fsspec-compatible library:

```python
import pandas as pd

storage_options = {"base_url": "https://your-alfresco-host", "username": "admin", "password": "admin"}
df = pd.read_csv("alfd:///Sites/mysite/documentLibrary/data.csv", storage_options=storage_options)
```

## Authentication

### Basic auth (default)

```python
fs = fsspec.filesystem("alfd", base_url="...", username="user", password="pass")
```

### OAuth2

```python
fs = fsspec.filesystem(
    "alfd",
    base_url="...",
    auth_type="oauth2",
    oauth2_client_params={
        "client_id": "...",
        "client_secret": "...",
        "token_endpoint": "https://your-idp/token",
    },
)
```

### Environment variables

You can avoid hardcoding credentials by setting environment variables:

| Variable | Description |
|---|---|
| `ALFRESCOFS_BASE_URL` | Alfresco base URL |
| `ALFRESCOFS_USERNAME` | Username (basic auth) |
| `ALFRESCOFS_PASSWORD` | Password (basic auth) |

## Running the tests

See [TESTING.md](TESTING.md).
