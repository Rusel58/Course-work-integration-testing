import time

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union

from infra.clients import base_offline_viewer_client as ov_base


@dataclass
class _DryRunExperiment:
    id: Union[int, str]
    name: Optional[str] = None
    branch_baseline: Optional[str] = None
    branch_interest: Optional[str] = None
    dataset: Optional[str] = None
    author: Optional[str] = None
    attributes: Any = None
    status: str = ov_base.OV_STATUS_READY
    created_at: float = field(default_factory=time.time)


@dataclass
class _DryRunRun:
    id: Union[int, str]
    experiment_id: Union[int, str]
    status: str = ov_base.OV_STATUS_ENQUEUED
    commit_hash: Optional[str] = None
    commit_date: Optional[int] = None
    attributes: Any = None
    scenes_total: Optional[int] = None
    scenes_dropped: Optional[int] = None
    scenes_failure: Optional[int] = None
    scenes_simulated: Optional[int] = None
    created_at: float = field(default_factory=time.time)


class OfflineViewerDryRunClient(ov_base.BaseOfflineViewerClient):
    """
    Dry-run client:
    - does not go online
    - stores data in-memory
    - returns JSON with the fields status, metrics_runs, attributes, etc.
    """

    def __init__(
        self,
        offline_viewer_host_url: Optional[str] = None,
        default_status: str = ov_base.OV_STATUS_READY,
        finalize_on_iteration: int = 2,
        current_iteration: int = 0,
    ):
        self._ov_host_url = offline_viewer_host_url or ov_base.OV_DEFAULT_HOST_URL
        self._experiments: Dict[Union[int, str], _DryRunExperiment] = {}
        self._runs: Dict[Union[int, str], _DryRunRun] = {}
        self._experiment_id_counter = 10_000
        self._run_id_counter = 20_000
        self._default_status = default_status
        self._finalize_on_iter = int(finalize_on_iteration)
        self._iter = int(current_iteration)

    def ui_link_prefix(self) -> str:
        return ov_base.make_ui_link_prefix(self._ov_host_url)

    def _next_experiment_id(self) -> int:
        self._experiment_id_counter += 1
        return self._experiment_id_counter

    def _next_run_id(self) -> int:
        self._run_id_counter += 1
        return self._run_id_counter

    def create_experiment(
        self,
        name: Optional[str] = None,
        branch_baseline: Optional[str] = None,
        branch_interest: Optional[str] = None,
        dataset: Optional[str] = None,
        author: Optional[str] = None,
        attributes: Optional[Any] = None,
    ) -> Any:
        exp_id = self._next_experiment_id()
        exp = _DryRunExperiment(
            id=exp_id,
            name=name,
            branch_baseline=branch_baseline,
            branch_interest=branch_interest,
            dataset=dataset,
            author=author,
            attributes=attributes,
        )
        self._experiments[exp_id] = exp
        return self._experiment_to_dict(exp)

    def create_run(
        self,
        experiment_id: Optional[Union[int, str]] = None,
        commit_hash: Optional[str] = None,
        commit_date: Optional[int] = None,
        attributes: Optional[Any] = None,
    ) -> Any:
        run_id = self._next_run_id()
        run = _DryRunRun(
            id=run_id,
            experiment_id=experiment_id,
            commit_hash=commit_hash,
            commit_date=commit_date,
            attributes=attributes,
        )
        self._runs[run_id] = run
        return self._run_to_dict(run)

    def _update_fields_if_not_none(self, run: _DryRunRun, **kwargs: Any) -> None:
        for field_name, value in kwargs.items():
            if value is not None:
                setattr(run, field_name, value)

    def update_run(
        self,
        run_id: Union[int, str],
        status: Optional[str] = None,
        pulsar_instance: Optional[str] = None,
        attributes: Optional[Any] = None,
        scenes_total: Optional[int] = None,
        scenes_dropped: Optional[int] = None,
        scenes_failure: Optional[int] = None,
        scenes_simulated: Optional[int] = None,
    ) -> Any:
        run = self._runs.get(run_id)
        if run is None:
            raise Exception(f"DryRun OV: run '{run_id}' not found")

        self._update_fields_if_not_none(
            run,
            status=status,
            attributes=attributes,
            scenes_total=scenes_total,
            scenes_dropped=scenes_dropped,
            scenes_failure=scenes_failure,
            scenes_simulated=scenes_simulated,
        )

        return self._run_to_dict(run)

    def get_run(self, run_id: Union[int, str]) -> Any:
        run = self._runs.get(run_id)
        if run is None:
            raise Exception(f"DryRun OV: run '{run_id}' not found")
        return self._run_to_dict(run)

    def get_experiment(self, exp_id: Union[int, str]) -> Any:
        exp = self._experiments.get(exp_id)

        if exp is None:
            exp = _DryRunExperiment(
                id=exp_id,
                name=f"Dry-run experiment {exp_id}",
                attributes={"dry_run": True},
                status="running",
            )
            self._experiments[exp_id] = exp

        if exp.status in {ov_base.OV_STATUS_READY, "success", ov_base.OV_STATUS_FAILED}:
            return self._experiment_to_dict(exp)

        if self._iter < self._finalize_on_iter:
            exp.status = "running"
            return self._experiment_to_dict(exp)

        exp.status = self._default_status
        return self._experiment_to_dict(exp)

    def _experiment_to_dict(self, exp: _DryRunExperiment) -> Dict[str, Any]:
        return {
            "id": exp.id,
            "name": exp.name,
            "branch_baseline": exp.branch_baseline,
            "branch_interest": exp.branch_interest,
            "dataset": exp.dataset,
            "author": exp.author,
            "attributes": exp.attributes,
            "status": exp.status,
            "metrics_runs": [],
            "dry_run": True,
        }

    def _run_to_dict(self, run: _DryRunRun) -> Dict[str, Any]:
        return {
            "id": run.id,
            "experiment_id": run.experiment_id,
            "status": run.status,
            "commit_hash": run.commit_hash,
            "commit_date": run.commit_date,
            "attributes": run.attributes,
            "scenes_total": run.scenes_total,
            "scenes_dropped": run.scenes_dropped,
            "scenes_failure": run.scenes_failure,
            "scenes_simulated": run.scenes_simulated,
            "dry_run": True,
        }
