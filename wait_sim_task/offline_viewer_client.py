from typing import Any, Dict, Mapping, Optional, Union

from core.infra.network import session
from infra.clients import base_offline_viewer_client as ov_base
from infra.utils.network.url_util import urljoin


class OfflineViewerClient(ov_base.BaseOfflineViewerClient):
    def __init__(self, offline_viewer_host_url: Optional[str] = None):
        self._session = None
        self._ov_host_url = offline_viewer_host_url or ov_base.OV_DEFAULT_HOST_URL

    @property
    def session(self):
        if self._session is None:
            self._session = session.RequestsSessionWithTimeoutsAndRetries(
                status_forcelist=(500, 502, 503, 504),
                allowed_methods=("GET", "PUT", "POST", "DELETE", "PATCH"),
                retries=7,
                backoff_factor=1.2,
                backoff_max=60,
            )
        return self._session

    def _clear_nones(self, data: Mapping[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in data.items() if v is not None}

    def create_experiment(
        self,
        name: Optional[str] = None,
        branch_baseline: Optional[str] = None,
        branch_interest: Optional[str] = None,
        dataset: Optional[str] = None,
        author: Optional[str] = None,
        attributes: Optional[Any] = None,
    ) -> Any:
        headers = {"Content-Type": "application/json"}

        data = {
            "name": name,
            "branch_baseline": branch_baseline,
            "branch_interest": branch_interest,
            "dataset": dataset,
            "author": author,
            "attributes": attributes,
        }
        data = self._clear_nones(data)

        try:
            resp = self.session.post(
                url=urljoin(
                    self._ov_host_url,
                    self.EXPERIMENT_PATHNAME,
                    "",
                ),
                headers=headers,
                json=data,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            raise Exception(
                f'Cannot create ov experiment for branch_baseline "{branch_baseline}": {e}',
            ) from e

    def create_run(
        self,
        experiment_id: Optional[Union[int, str]] = None,
        commit_hash: Optional[str] = None,
        commit_date: Optional[int] = None,
        attributes: Optional[Any] = None,
    ) -> Any:
        headers = {"Content-Type": "application/json"}

        data = {
            "status": ov_base.OV_STATUS_ENQUEUED,
            "experiment_id": experiment_id,
            "commit_hash": commit_hash,
            "commit_date": commit_date,
            "attributes": attributes,
        }
        data = self._clear_nones(data)

        try:
            resp = self.session.post(
                url=urljoin(
                    self._ov_host_url,
                    self.RUN_PATHNAME,
                    "",
                ),
                headers=headers,
                json=data,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            raise Exception(f'Cannot create ov run for experiment "{experiment_id}": {e}') from e

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
        headers = {"Content-Type": "application/json"}

        data = {
            "status": status,
            "pulsar_instance": pulsar_instance,
            "attributes": attributes,
            "scenes_total": scenes_total,
            "scenes_dropped": scenes_dropped,
            "scenes_simulated": scenes_simulated,
            "scenes_failure": scenes_failure,
        }
        data = self._clear_nones(data)

        try:
            resp = self.session.patch(
                url=urljoin(
                    self._ov_host_url,
                    self.RUN_PATHNAME,
                    str(run_id),
                    "",
                ),
                headers=headers,
                json=data,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            raise Exception(f'Cannot update ov run "{run_id}": {e}') from e

    def get_run(self, run_id: Union[int, str]) -> Any:
        headers = {"Content-Type": "application/json"}

        try:
            resp = self.session.post(
                url=urljoin(
                    self._ov_host_url,
                    self.RUN_PATHNAME,
                    str(run_id),
                ),
                headers=headers,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            raise Exception(f"Cannot get ov run: {e}") from e

    def get_experiment(self, exp_id: Union[int, str]) -> Any:
        headers = {"Content-Type": "application/json"}

        try:
            resp = self.session.get(
                url=urljoin(
                    self._ov_host_url,
                    self.EXPERIMENT_PATHNAME,
                    str(exp_id),
                ),
                headers=headers,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            raise Exception(f"Cannot get ov experiment: {e}") from e

    def ui_link_prefix(self) -> str:
        return ov_base.make_ui_link_prefix(self._ov_host_url)
