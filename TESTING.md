# Testing Guide

## Test structure

The suite is split into two layers:

| File | Type | Credentials required |
|------|------|----------------------|
| `tests/test_helpers.py` | Unit | No |
| `tests/test_constructor.py` | Unit | No |
| `tests/test_internals.py` | Unit | No |
| `tests/test_alfresco_api.py` | Unit | No |
| `tests/test_live_read.py` | Integration | Yes |
| `tests/test_live_crud.py` | Integration | Yes |
| `tests/test_live_write.py` | Integration | Yes |
| `tests/test_live_append.py` | Integration | Yes |
| `tests/test_live_versioning.py` | Integration | Yes |

Integration tests are skipped automatically when credentials are absent.

## Running tests

### Unit tests only

```bash
cd alfrescofs
.venv/bin/pytest tests/test_helpers.py tests/test_constructor.py tests/test_internals.py tests/test_alfresco_api.py
```

### Full suite

```bash
.venv/bin/pytest tests/
```

Integration tests will be skipped if credentials are not provided.

### Full suite with credentials

Via environment variables:

```bash
export ALFRESCOFS_BASE_URL=http://your-alfresco-host:8080
export ALFRESCOFS_USERNAME=admin
export ALFRESCOFS_PASSWORD=secret
.venv/bin/pytest tests/
```

Via pytest options:

```bash
`.venv/bin/pytest tests/ --base-url http://your-alfresco-host:8080 --username admin --password secret`
```

## Configuration

Test configuration is defined in `pyproject.toml` under `[tool.pytest.ini_options]`.
