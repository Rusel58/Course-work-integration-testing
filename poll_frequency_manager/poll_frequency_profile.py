import re
from enum import Enum
from types import MappingProxyType
from typing import Iterable, Optional, NamedTuple, Mapping
from .poll_frequency_manager import PollFrequencyManager
import logging

logger = logging.getLogger(__name__)


class PollFrequencyProfile(NamedTuple):
    transition_duration: int
    initial_poll_freq: int
    final_poll_freq: int


class PollProfile(Enum):
    MEDIUM = PollFrequencyProfile(transition_duration=9000, initial_poll_freq=1800, final_poll_freq=600)
    LONG = PollFrequencyProfile(transition_duration=18000, initial_poll_freq=5400, final_poll_freq=900)
    DEFAULT = PollFrequencyProfile(transition_duration=1800, initial_poll_freq=300, final_poll_freq=300)
    DRY_RUN = PollFrequencyProfile(transition_duration=1800, initial_poll_freq=60, final_poll_freq=60)

    @classmethod
    def from_str(cls, name: Optional[str]) -> Optional[PollFrequencyProfile]:
        if not name:
            return None
        key = str(name).strip().upper()
        try:
            return cls[key].value
        except KeyError:
            return None

    @classmethod
    def names(cls) -> tuple[str, ...]:
        return tuple(m.name for m in cls if m.name != "DRY_RUN")

    @classmethod
    def to_mapping(cls) -> Mapping[str, PollFrequencyProfile]:
        return MappingProxyType({m.name: m.value for m in cls})


MEDIUM = PollProfile.MEDIUM.value
LONG = PollProfile.LONG.value
DEFAULT = PollProfile.DEFAULT.value

REGEX_RULES: Mapping[str, PollFrequencyProfile] = MappingProxyType(
    {
        r"^RELEASE:": MEDIUM,
        r"^SDC_LONG_DURATION_FLOW$": MEDIUM,
    }
)


def resolve_profile_from_name(name: Optional[str]) -> Optional[PollFrequencyProfile]:
    """
    From the explicit profile name (MEDIUM/LONG/DEFAULT), return the profile, otherwise None.
    """
    return PollProfile.from_str(name)


def effective_profile(
    *,
    name: Optional[str],
    initial_poll_freq: Optional[int],
    poll_freq: int,
    transition_duration: int,
    tags: Iterable[str],
) -> PollFrequencyProfile:
    """
    A single point for calculating the final profile.

    Priority:
      1) If a valid name (MEDIUM/LONG/DEFAULT) is set → return it.
      2) Otherwise, if initial_poll_freq is None And poll_freq == DEFAULT.final_poll_freq → try by tags;
         (resolve_profile_from_tags returns MEDIUM or DEFAULT).
      3) Otherwise, build a profile from explicit numeric parameters.
    It always returns the profile.
    """
    choices = PollProfile.names()
    if name is not None:
        key = str(name).strip().upper()
        if key not in choices:
            logging.warning(
                "Unknown poll_freq_profile=%r; expected one of [%s]. Falling back.",
                name,
                ", ".join(choices),
            )
    by_name = resolve_profile_from_name(name)
    if by_name is not None:
        return by_name

    if initial_poll_freq is None and int(poll_freq) == DEFAULT.final_poll_freq:
        return resolve_profile_from_tags(tags)

    fp = int(poll_freq)
    td = int(transition_duration)
    ip = int(initial_poll_freq) if initial_poll_freq is not None else fp
    PollFrequencyManager.check_input_parameters(transition_duration=td, initial_poll_freq=ip, final_poll_freq=fp)
    return PollFrequencyProfile(transition_duration=td, initial_poll_freq=ip, final_poll_freq=fp)


def resolve_profile_from_tags(task_tags: Iterable[str]) -> PollFrequencyProfile:
    """
    Priority:
      1) PREFIX_RULES (first match by startswith)
      2) EXACT_RULES (first exact match)
      3) no matches → DEFAULT
    """
    if not task_tags:
        return DEFAULT

    for raw in task_tags:
        tag = "" if raw is None else str(raw).strip()
        if not tag:
            continue
        for pattern, profile in REGEX_RULES.items():
            if re.match(pattern, tag):
                return profile
    return DEFAULT

