import pytest

from infra.clients import base_offline_viewer_client as ov_base
from infra.clients.offline_viewer_dry_run_client import OfflineViewerDryRunClient


@pytest.fixture
def client() -> OfflineViewerDryRunClient:
    return OfflineViewerDryRunClient(
        offline_viewer_host_url=ov_base.OV_DEFAULT_HOST_URL,
        default_status=ov_base.OV_STATUS_READY,
        finalize_on_iteration=2,
        current_iteration=0,
    )


def test_ui_link_prefix_uses_make_ui_link_prefix():
    custom_url = "https://<INTERNAL_DOMAIN>/rest/offline_viewer"
    client = OfflineViewerDryRunClient(offline_viewer_host_url=custom_url)
    assert client.ui_link_prefix() == "https://<INTERNAL_DOMAIN>/offline-viewer"


def test_get_experiment_first_iteration_running(client: OfflineViewerDryRunClient):
    client = OfflineViewerDryRunClient(
        offline_viewer_host_url=ov_base.OV_DEFAULT_HOST_URL,
        default_status=ov_base.OV_STATUS_READY,
        finalize_on_iteration=2,
        current_iteration=1,
    )
    exp = client.get_experiment("exp-1")

    assert exp["id"] == "exp-1"
    assert exp["status"] == "running"
    assert exp["dry_run"] is True
    assert "exp-1" in client._experiments


def test_get_experiment_second_iteration_finalizes(client: OfflineViewerDryRunClient):
    client_running = OfflineViewerDryRunClient(
        offline_viewer_host_url=ov_base.OV_DEFAULT_HOST_URL,
        default_status=ov_base.OV_STATUS_READY,
        finalize_on_iteration=2,
        current_iteration=1,
    )
    exp1 = client_running.get_experiment("exp-2")
    assert exp1["status"] == "running"

    client_ready = OfflineViewerDryRunClient(
        offline_viewer_host_url=ov_base.OV_DEFAULT_HOST_URL,
        default_status=ov_base.OV_STATUS_READY,
        finalize_on_iteration=2,
        current_iteration=2,
    )
    exp2 = client_ready.get_experiment("exp-2")
    assert exp2["status"] == ov_base.OV_STATUS_READY

    exp3 = client_ready.get_experiment("exp-2")
    assert exp3["status"] == ov_base.OV_STATUS_READY


def test_get_experiment_custom_final_status():
    client = OfflineViewerDryRunClient(
        default_status=ov_base.OV_STATUS_FAILED,
        finalize_on_iteration=1,
        current_iteration=1,
    )
    exp = client.get_experiment("exp-fail")
    assert exp["status"] == ov_base.OV_STATUS_FAILED


def test_create_experiment_uses_experiment_to_dict(client: OfflineViewerDryRunClient):
    exp = client.create_experiment(
        name="My experiment",
        branch_baseline="trunk",
        branch_interest="feature",
        dataset="dataset-id",
        author="tester",
        attributes={"foo": "bar"},
    )

    assert exp["name"] == "My experiment"
    assert exp["branch_baseline"] == "trunk"
    assert exp["branch_interest"] == "feature"
    assert exp["dataset"] == "dataset-id"
    assert exp["author"] == "tester"
    assert exp["attributes"] == {"foo": "bar"}
    assert exp["status"] == ov_base.OV_STATUS_READY
    assert exp["metrics_runs"] == []
    assert exp["dry_run"] is True


def test_create_and_get_run(client: OfflineViewerDryRunClient):
    run = client.create_run(
        experiment_id="exp-3",
        commit_hash="abcdef",
        commit_date=1234567890,
        attributes={"k": "v"},
    )
    run_id = run["id"]

    fetched = client.get_run(run_id)

    assert fetched["id"] == run_id
    assert fetched["experiment_id"] == "exp-3"
    assert fetched["commit_hash"] == "abcdef"
    assert fetched["commit_date"] == 1234567890
    assert fetched["attributes"] == {"k": "v"}
    assert fetched["status"] == ov_base.OV_STATUS_ENQUEUED
    assert fetched["dry_run"] is True


def test_update_run_updates_only_non_none_fields(client: OfflineViewerDryRunClient):
    run = client.create_run(
        experiment_id="exp-4",
        commit_hash="hash1",
        commit_date=111,
        attributes={"a": 1},
    )
    run_id = run["id"]

    client.update_run(
        run_id=run_id,
        status="success",
        pulsar_instance=None,
        attributes={"a": 2},
        scenes_total=10,
        scenes_dropped=None,
        scenes_failure=1,
        scenes_simulated=9,
    )

    updated = client.get_run(run_id)

    assert updated["status"] == "success"
    assert updated["attributes"] == {"a": 2}
    assert updated["scenes_total"] == 10
    assert updated["scenes_failure"] == 1
    assert updated["scenes_simulated"] == 9
    assert updated["scenes_dropped"] is None


def test_get_run_unknown_raises(client: OfflineViewerDryRunClient):
    with pytest.raises(Exception) as exc:
        client.get_run("non-existing-id")
    assert "DryRun OV: run 'non-existing-id' not found" in str(exc.value)
