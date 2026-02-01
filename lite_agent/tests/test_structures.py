import json
import os
import requests_mock

import yatest.common

from sandbox.projects.sdc.common.lite_agent_api.client import LiteAgentClient
from sandbox.projects.sdc.common.lite_agent_api.lite_agent_urls import STABLE_URL
from sandbox.projects.sdc.common.lite_agent_api.spawn_task import ArtifactDirectLink, SpawnTask
from sandbox.projects.sdc.common.lite_agent_api.task_steps_data import TaskStepsData
from sandbox.projects.sdc.common.lite_agent_api.task_state import TaskState


def test_steps_result():
    json_data = _load_json_from_file("get_steps_result.json")
    obj = TaskStepsData.from_json(json_data)

    assert len(obj.get_runtime_parameters()) == 18
    assert len(obj.get_runtime_statistics()) == 7
    assert len(obj.get_step_log_links()) == 4
    assert len([link for link in obj.get_step_log_links() if link.from_failed_step]) == 1
    assert len([link.step_duration for link in obj.get_step_log_links()]) == 4
    assert len(obj.get_build_problems()) == 2


def test_task_state():
    dict_obj = _load_json_from_file("get_task_state.json")
    state = TaskState.from_dict(dict_obj, STABLE_URL)
    assert state.get_status() == "success"
    assert state.is_success()
    assert not state.is_failure()
    assert not state.in_progress()
    assert state.get_task_id() == "1"
    assert state.get_task_id() in state.get_task_url()
    assert int(state.get_creation_time().timestamp()) == 1709053565
    assert int(state.get_start_time().timestamp()) == 1709053573
    assert int(state.get_finish_time().timestamp()) == 1709053662


def test_spawn_task_dto():
    dto = SpawnTask.create(
        task_type="run_ci_script",
        branch="trunk",
        commit="full_commit_hash",
        ttl_logs=10,
        files_with_direct_link=[
            ArtifactDirectLink(
                "ci_report.html",
                "ci_report.html",
                10,
                "SDC_BUILD_SCRIPT_FILE_ARTIFACT",
                "report with ci errors",
                {
                    "name": "ci_report.html",
                    "task_type": "SDC_LITE_AGENT_TASK",
                    "task_id": "1234342",
                },
            )
        ],
        artifact_zip_params=ArtifactDirectLink(
            "artifacts",
            "artifacts",
            2,
            "SDC_BUILD_SCRIPT_ARTIFACTS",
            "archive with all artifacts",
            {
                "name": "artifacts.zip",
                "task_type": "SDC_LITE_AGENT_TASK",
                "task_id": "1234342",
            },
        ),
        tag_filters=["ci_tests"],
        env_variables={"test_variable_name": "test_variable_value"},
        arc_vcs_project_dir="sdg/sdc",
    )

    dict_res = dto.to_dict()

    expected = {
        "filters": ["ci_tests"],
        "params": {
            "env": {
                "BRANCH": "trunk",
                "COMMIT": "full_commit_hash",
                "SDC_ARC_DIR": "arcadia",
                "SDC_PROJECT_DIR": "sdg/sdc",
                "test_variable_name": "test_variable_value",
            },
            "resources-with-direct-link": [
                {
                    "name": "ci_report.html",
                    "ttl": 10,
                    "wildcard": "ci_report.html",
                    "type": "SDC_BUILD_SCRIPT_FILE_ARTIFACT",
                    "description": "report with ci errors",
                    "attributes": {
                        "name": "ci_report.html",
                        "task_type": "SDC_LITE_AGENT_TASK",
                        "task_id": "1234342",
                    },
                }
            ],
            "artifact_zip_params": {
                "name": "artifacts",
                "ttl": 2,
                "type": "SDC_BUILD_SCRIPT_ARTIFACTS",
                "description": "archive with all artifacts",
                "attributes": {
                    "name": "artifacts.zip",
                    "task_type": "SDC_LITE_AGENT_TASK",
                    "task_id": "1234342",
                },
            },
            "ttl_logs": 10,
        },
        "type": "run_ci_script",
    }

    assert dict_res == expected


def _load_json_from_file(file_name):
    base_path = yatest.common.test_source_path()
    full_path = os.path.join(base_path, "data", file_name)
    with open(full_path) as fd:
        return json.load(fd)


def test_get_target_daemon_version():
    client = LiteAgentClient(base_url=STABLE_URL, token="<REDACTED>")
    json_data = _load_json_from_file("get_target_daemon_version.json")
    with requests_mock.Mocker() as m:
        m.get("https://<INTERNAL_DOMAIN>/admin/target-daemon-version", json=json_data)
        version = client.get_target_daemon_version()

    assert version == "1.2.34567"

