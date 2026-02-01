STABLE_URL = "https://<INTERNAL_DOMAIN>"
PRESTABLE_URL = "https://<INTERNAL_DOMAIN>"
UNSTABLE_URL = "https://<INTERNAL_DOMAIN>"

URLS_BY_NAME = {
    "stable": STABLE_URL,
    "prestable": PRESTABLE_URL,
    "unstable": UNSTABLE_URL,
}


def resolve_base_url(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("api_type must be a non-empty string")
    key = name.strip().lower()
    if key not in URLS_BY_NAME:
        allowed = ", ".join(URLS_BY_NAME.keys())
        raise ValueError(f"Unknown api_type: {name!r}. Allowed: {allowed}")
    return URLS_BY_NAME[key]

