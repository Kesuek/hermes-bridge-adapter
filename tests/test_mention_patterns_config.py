"""Mention patterns / allowed users configured as YAML lists.

Regression: BridgeConfig._get_opt() returned ``str(val)``, so a YAML list such as

    platforms.bridge-adapter.extra.mention_patterns:
      - (?i)@hermes
      - '(?i)hermes:'

reached _compile_mention_patterns() as the Python repr
"['(?i)@hermes', '(?i)hermes:']". That is not JSON, so it fell back to a comma
split and compiled "['(?i)@hermes'" -> re.error "unterminated character set at
position 0". The exception escaped _poll_all(), so every group-chat message that
needed a mention check was left unprocessed in the inbox.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import adapter  # noqa: E402
from adapter import BridgeAdapter, BridgeConfig  # noqa: E402

YAML_LIST = ["(?i)@hermes", "(?i)hermes:"]


def _compiled(raw):
    return BridgeAdapter._compile_mention_patterns(raw)


def _matches(patterns, text):
    return any(p.search(text) for p in patterns)


def test_yaml_list_from_config_compiles_and_matches():
    bc = BridgeConfig("imsg", {"mention_patterns": YAML_LIST})
    patterns = _compiled(bc.mention_patterns)
    assert len(patterns) == 2
    assert _matches(patterns, "@Hermes can you check this")
    assert _matches(patterns, "hermes: summarize the thread")
    assert not _matches(patterns, "see you tomorrow")


def test_bridge_specific_yaml_list_wins_over_global():
    bc = BridgeConfig(
        "imsg",
        {"bridge_imsg_mention_patterns": ["(?i)@herm"], "mention_patterns": ["nomatch"]},
    )
    patterns = _compiled(bc.mention_patterns)
    assert _matches(patterns, "@herm hi")
    assert not _matches(patterns, "nomatch")


def test_pattern_containing_comma_survives_yaml_list():
    # A comma inside a regex must not be split into two broken patterns.
    bc = BridgeConfig("imsg", {"mention_patterns": [r"(?i)\bher{1,2}mes\b"]})
    patterns = _compiled(bc.mention_patterns)
    assert len(patterns) == 1
    assert _matches(patterns, "hermes ping")


def test_json_string_still_supported():
    patterns = _compiled('["(?i)@hermes", "(?i)hermes:"]')
    assert len(patterns) == 2


def test_python_repr_string_is_parsed_not_split():
    # Defensive: a list that was already stringified with str() upstream.
    patterns = _compiled("['(?i)@hermes', '(?i)hermes:']")
    assert len(patterns) == 2
    assert _matches(patterns, "@hermes hi")


def test_comma_separated_env_string_still_supported():
    patterns = _compiled("(?i)@hermes,(?i)hermes:")
    assert len(patterns) == 2


def test_none_uses_defaults():
    patterns = _compiled(None)
    assert len(patterns) == len(adapter.DEFAULT_MENTION_PATTERNS)
    assert _matches(patterns, "hermes, are you there")


def test_invalid_pattern_is_skipped_not_raised():
    patterns = _compiled(["[unclosed", "(?i)@hermes"])
    assert len(patterns) == 1
    assert _matches(patterns, "@hermes hi")


def test_is_mentioned_does_not_raise_with_yaml_list(tmp_path):
    a = BridgeAdapter.__new__(BridgeAdapter)
    a._bridge_configs = {"imsg": BridgeConfig("imsg", {"mention_patterns": YAML_LIST})}
    assert a._is_mentioned("@hermes what's on my calendar", "imsg") is True
    assert a._is_mentioned("lunch at 1?", "imsg") is False


def test_allowed_users_yaml_list():
    bc = BridgeConfig("imsg", {"allowed_users": ["a@example.com", "+15551234567"]})
    assert bc.is_user_allowed("a@example.com")
    assert bc.is_user_allowed("+15551234567")
    assert not bc.is_user_allowed("c@example.com")


def test_allowed_users_comma_string_unchanged():
    bc = BridgeConfig("imsg", {"allowed_users": "a@example.com, b@example.com"})
    assert bc.is_user_allowed("b@example.com")
