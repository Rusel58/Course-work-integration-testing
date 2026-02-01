import pytest

from poll_frequency_manager import poll_frequency_profile

# ---------- Enum/constants ----------


def test_constants_mirror_enum():
    assert poll_frequency_profile.DEFAULT == poll_frequency_profile.PollProfile.DEFAULT.value
    assert poll_frequency_profile.MEDIUM == poll_frequency_profile.PollProfile.MEDIUM.value


def test_names_contains_all_in_order():
    assert poll_frequency_profile.PollProfile.names() == ("MEDIUM", "LONG", "DEFAULT")


def test_to_mapping_readonly_and_values():
    mapping = poll_frequency_profile.PollProfile.to_mapping()
    assert mapping["DEFAULT"] == poll_frequency_profile.PollProfile.DEFAULT.value
    with pytest.raises(TypeError):
        mapping["X"] = poll_frequency_profile.PollFrequencyProfile(1, 1, 1)


# ---------- from_str / resolve_profile_from_name ----------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("MEDIUM", poll_frequency_profile.PollProfile.MEDIUM.value),
        ("medium", poll_frequency_profile.PollProfile.MEDIUM.value),
        ("  Medium  ", poll_frequency_profile.PollProfile.MEDIUM.value),
        ("LONG", poll_frequency_profile.PollProfile.LONG.value),
        ("DEFAULT", poll_frequency_profile.PollProfile.DEFAULT.value),
    ],
)
def test_from_str_and_resolve_profile_from_name(raw, expected):
    assert poll_frequency_profile.PollProfile.from_str(raw) == expected
    assert poll_frequency_profile.resolve_profile_from_name(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "   ", "UNKNOWN", "min", "looooong"])
def test_from_str_invalid_or_blank(raw):
    assert poll_frequency_profile.PollProfile.from_str(raw) is None
    assert poll_frequency_profile.resolve_profile_from_name(raw) is None


# ---------- resolve_profile_from_tags ----------


@pytest.mark.parametrize("tags", [[], tuple(), None])
def test_returns_default_when_no_tags(tags):
    assert poll_frequency_profile.resolve_profile_from_tags(tags) == poll_frequency_profile.DEFAULT


@pytest.mark.parametrize(
    "tags",
    [
        ["RELEASE:CAR_MIDNIGHT_QCD_RELEASE"],
        ["   RELEASE:Something   "],
        ["foo", "RELEASE:XYZ"],
        [None, "  ", "RELEASE:"],
    ],
)
def test_matches_release_prefix(tags):
    assert poll_frequency_profile.resolve_profile_from_tags(tags) == poll_frequency_profile.MEDIUM


@pytest.mark.parametrize(
    "tags",
    [
        ["SDC_LONG_DURATION_FLOW"],
        ["foo", "bar", "SDC_LONG_DURATION_FLOW"],
        ["  SDC_LONG_DURATION_FLOW  "],
    ],
)
def test_matches_exact_tag(tags):
    assert poll_frequency_profile.resolve_profile_from_tags(tags) == poll_frequency_profile.MEDIUM


def test_prefers_any_match_over_default_when_mixed_with_noise():
    tags = [None, "", "   ", "foo", "RELEASE:BUILD_42"]
    assert poll_frequency_profile.resolve_profile_from_tags(tags) == poll_frequency_profile.MEDIUM


def test_returns_default_on_non_matching_tags():
    tags = ["FOO", "BAR", "BAZ"]
    assert poll_frequency_profile.resolve_profile_from_tags(tags) == poll_frequency_profile.DEFAULT


def test_case_sensitive_no_match():
    tags = ["release:abc", "sdc_long_duration_flow"]
    assert poll_frequency_profile.resolve_profile_from_tags(tags) == poll_frequency_profile.DEFAULT


def test_tags_can_be_any_iterable_generator():
    tags = (t for t in ["noise", "RELEASE:Z"])
    assert poll_frequency_profile.resolve_profile_from_tags(tags) == poll_frequency_profile.MEDIUM


# ---------- effective_profile ----------


def test_effective_profile_respects_name_override():
    prof = poll_frequency_profile.effective_profile(
        name="MEDIUM",
        initial_poll_freq=None,
        poll_freq=1,
        transition_duration=2,
        tags=["FOO"],
    )
    assert prof == poll_frequency_profile.PollProfile.MEDIUM.value


def test_effective_profile_allows_explicit_default_by_name():
    prof = poll_frequency_profile.effective_profile(
        name="DEFAULT",
        initial_poll_freq=None,
        poll_freq=123,
        transition_duration=456,
        tags=[],
    )
    assert prof == poll_frequency_profile.PollProfile.DEFAULT.value


def test_effective_profile_uses_tags_when_ipf_none_and_pf_equals_default():
    prof = poll_frequency_profile.effective_profile(
        name=None,
        initial_poll_freq=None,
        poll_freq=poll_frequency_profile.DEFAULT.final_poll_freq,
        transition_duration=999,
        tags=["RELEASE:ANY"],
    )
    assert prof == poll_frequency_profile.MEDIUM


def test_effective_profile_uses_default_when_tags_absent_in_tag_branch():
    prof = poll_frequency_profile.effective_profile(
        name=None,
        initial_poll_freq=None,
        poll_freq=poll_frequency_profile.DEFAULT.final_poll_freq,
        transition_duration=123,
        tags=None,
    )
    assert prof == poll_frequency_profile.DEFAULT


def test_effective_profile_builds_numeric_profile_when_ipf_set():
    prof = poll_frequency_profile.effective_profile(
        name=None,
        initial_poll_freq=420,
        poll_freq=300,
        transition_duration=600,
        tags=["RELEASE:ANY"],
    )
    assert prof == poll_frequency_profile.PollFrequencyProfile(
        transition_duration=600, initial_poll_freq=420, final_poll_freq=300
    )


def test_effective_profile_builds_numeric_profile_when_pf_not_default():
    prof = poll_frequency_profile.effective_profile(
        name=None,
        initial_poll_freq=None,
        poll_freq=450,
        transition_duration=900,
        tags=["RELEASE:ANY"],
    )
    assert prof == poll_frequency_profile.PollFrequencyProfile(
        transition_duration=900, initial_poll_freq=450, final_poll_freq=450
    )


def test_effective_profile_warns_on_invalid_name(caplog):
    with caplog.at_level("WARNING"):
        prof = poll_frequency_profile.effective_profile(
            name="weird",
            initial_poll_freq=None,
            poll_freq=poll_frequency_profile.DEFAULT.final_poll_freq,
            transition_duration=100,
            tags=[],
        )
    assert prof == poll_frequency_profile.DEFAULT
    assert any("Unknown poll_freq_profile" in rec.getMessage() for rec in caplog.records)

