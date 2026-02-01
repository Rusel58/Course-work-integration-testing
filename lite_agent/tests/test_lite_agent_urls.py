import pytest

from sandbox.projects.sdc.common.lite_agent_api.lite_agent_urls import (
    resolve_base_url,
    URLS_BY_NAME,
    STABLE_URL,
    PRESTABLE_URL,
    UNSTABLE_URL,
)


@pytest.mark.parametrize(
    "name, expected",
    [
        ("stable", STABLE_URL),
        ("prestable", PRESTABLE_URL),
        ("unstable", UNSTABLE_URL),
        ("  STABLE  ", STABLE_URL),
    ],
)
def test_resolve_base_url_ok(name, expected):
    assert resolve_base_url(name) == expected


@pytest.mark.parametrize("bad", [None, "", "  ", 123, "unknown", "prod"])
def test_resolve_base_url_invalid_raises(bad):
    with pytest.raises(ValueError):
        resolve_base_url(bad)


def test_urls_by_name_has_required_keys():
    for key in ("stable", "prestable", "unstable"):
        assert key in URLS_BY_NAME

