import pytest
from sdg.ci.sandbox.nirvana.sdc_run_nirvana_workflow import DryRunNirvanaClient


def test_initialization():
    client = DryRunNirvanaClient(iteration=1)
    assert client.url == "https://nirvana.yandex-team.ru/api/public/v1/"
    assert client.iteration == 1
    assert "cloneWorkflowInstance" in client.mock_data


def test_download_resource():
    client = DryRunNirvanaClient(iteration=1)
    result = client.download_resource("any_url")
    assert result == {"mock_key": "mock_value"}


def test_make_request_valid_url():
    client = DryRunNirvanaClient(iteration=1)
    result = client.make_request("cloneWorkflowInstance", {})
    assert result == {"result": "dry_run"}


def test_make_request_invalid_url():
    client = DryRunNirvanaClient(iteration=1)
    with pytest.raises(Exception) as exc_info:
        client.make_request("invalid_url", {})
    assert "No mock data available for URL: invalid_url" in str(exc_info.value)


def test_get_execution_state_not_completed():
    client = DryRunNirvanaClient(iteration=1)
    result = client.make_request("getExecutionState", {})
    assert result["status"] == "not completed"


def test_get_execution_state_completed_after_iteration():
    client = DryRunNirvanaClient(iteration=2)
    result = client.make_request("getExecutionState", {})
    assert result["status"] == "completed"


def test_workflow_completed_with_no_success():
    client = DryRunNirvanaClient(iteration=1)
    client.mock_data["getExecutionState"] = {"status": "completed", "result": "failed"}
    progress = client.make_request("getExecutionState", {})
    assert progress["status"] == "completed"
    assert progress["result"] == "failed"


def test_workflow_completed_successfully():
    client = DryRunNirvanaClient(iteration=1)
    client.mock_data["getExecutionState"] = {"status": "completed", "result": "success"}
    progress = client.make_request("getExecutionState", {})
    assert progress["status"] == "completed"
    assert progress["result"] == "success"


def test_modify_mock_data():
    client = DryRunNirvanaClient(iteration=1)
    original_data = client.mock_data["cloneWorkflowInstance"]
    client.mock_data["cloneWorkflowInstance"] = {"result": "modified_result"}
    result = client.make_request("cloneWorkflowInstance", {})
    assert result == {"result": "modified_result"}
    client.mock_data["cloneWorkflowInstance"] = original_data


def test_make_request_with_params():
    client = DryRunNirvanaClient(iteration=1)
    result = client.make_request("cloneWorkflowInstance", {"param1": "value1", "param2": "value2"})
    assert result == {"result": "dry_run"}


def test_iteration_effect_on_mock_data():
    client_first_iteration = DryRunNirvanaClient(iteration=1)
    client_second_iteration = DryRunNirvanaClient(iteration=2)

    result_first = client_first_iteration.make_request("getExecutionState", {})
    result_second = client_second_iteration.make_request("getExecutionState", {})

    assert result_first["status"] == "not completed"
    assert result_second["status"] == "completed"


def test_empty_params():
    client = DryRunNirvanaClient(iteration=1)
    result = client.make_request("cloneWorkflowInstance", {})
    assert result == {"result": "dry_run"}

    result = client.make_request("cloneWorkflowInstance", None)
    assert result == {"result": "dry_run"}
