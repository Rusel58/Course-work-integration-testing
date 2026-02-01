import re
from datetime import datetime

import pytest
import sandbox.projects.sdc.common.lite_agent_api.dry_run_client as dry_mod

from sandbox.projects.sdc.common.lite_agent_api.dry_run_client import LiteAgentDryRunClient
from sandbox.projects.sdc.common.lite_agent_api.lite_agent_urls import (
    STABLE_URL,
    PRESTABLE_URL,
    UNSTABLE_URL,
)
from sandbox.projects.sdc.common.lite_agent_api.task_steps_data import UPLOAD_ARTIFACTS_TO_SANDBOX_STEP_NAME


@pytest.fixture
def steps_identity(monkeypatch):
    monkeypatch.setattr(dry_mod.TaskStepsData, "from_json", staticmethod(lambda payload: payload))


def _mk_client(iteration=0, finalize_on=2, base_url=STABLE_URL, status="success"):
    return LiteAgentDryRunClient(
        base_url=base_url,
        current_iteration=iteration,
        finalize_on_iteration=finalize_on,
        default_status=status,
    )


def test_new_instance_helpers_use_urls():
    assert _mk_client(base_url=STABLE_URL).base_url == STABLE_URL
    assert _mk_client(base_url=PRESTABLE_URL).base_url == PRESTABLE_URL
    assert _mk_client(base_url=UNSTABLE_URL).base_url == UNSTABLE_URL


def test_create_task_and_in_progress_before_threshold():
    c = _mk_client(iteration=1, finalize_on=3)
    st = c.create_task({"fqdn": "agent1"})
    got = c.get_task_state(st.get_task_id())
    assert got.get_status() == "in_progress"
    assert got.get_agent_fqdn() == "agent1"
    assert got.get_task_url().endswith(f"{got.get_task_id()}/status")


def test_finalize_on_threshold_sets_finish_time():
    c = _mk_client(iteration=3, finalize_on=3, status="success")
    st = c.create_task({})
    got = c.get_task_state(st.get_task_id())
    assert got.get_status() == "success"
    assert got.get_finish_time() is not None
    assert isinstance(got.get_finish_time(), datetime)


def test_cancel_sets_cancel_and_finish_time(steps_identity):
    c = _mk_client(iteration=0, finalize_on=99)
    st = c.create_task({})
    assert c.cancel_task(st.get_task_id()) is True
    got = c.get_task_state(st.get_task_id())
    assert got.get_status() == "cancel"
    assert got.get_finish_time() is not None

    steps = c.get_steps_result(st.get_task_id())
    assert isinstance(steps, list)
    names = [s["name"] for s in steps]
    assert UPLOAD_ARTIFACTS_TO_SANDBOX_STEP_NAME in names


def test_steps_payload_structure_and_links(steps_identity):
    base_url = "https://example.local"
    c = _mk_client(iteration=5, finalize_on=2, base_url=base_url)
    st = c.create_task({})
    task_id = st.get_task_id()

    payload = c.get_steps_result(task_id)
    by_name = {s["name"]: s for s in payload}

    for step in payload:
        assert "resources" in step and "logs" in step["resources"]
        assert step["resources"]["logs"]["link"].startswith(base_url.rstrip("/"))

    upload = by_name[UPLOAD_ARTIFACTS_TO_SANDBOX_STEP_NAME]
    artifacts_link = upload["resources"]["artifacts"]["link"]
    assert re.match(r"https://sandbox\.yandex-team\.ru/task/.*/artifact/artifacts\.zip", artifacts_link)


def test_store_is_isolated_by_base_url():
    c1 = _mk_client(base_url="https://a")
    c2 = _mk_client(base_url="https://b")

    t1 = c1.create_task({})
    t2 = c2.create_task({})

    s1 = c1.get_task_state(t1.get_task_id())
    s2 = c2.get_task_state(t2.get_task_id())

    assert s1.api_url == "https://a"
    assert s2.api_url == "https://b"
    assert t1.get_task_id() != t2.get_task_id()

