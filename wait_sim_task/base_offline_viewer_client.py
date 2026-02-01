from abc import ABC, abstractmethod
from typing import Any, Optional, Union

from infra.utils.network.url_util import urljoin

OV_STATUS_ENQUEUED = "enqueued"
OV_STATUS_RUNNING = "running"
OV_STATUS_READY = "ready"
OV_STATUS_OUTDATED = "outdated"
OV_STATUS_WRONG = "wrong"
OV_STATUS_FAILED = "failed"

OV_DEFAULT_HOST_URL = "https://<INTERNAL_DOMAIN>/rest/offline_viewer"


def make_ui_link_prefix(rest_url: str) -> str:
    """Convert REST base URL to UI base URL."""
    return rest_url.replace("/rest/offline_viewer", "/offline-viewer")


class BaseOfflineViewerClient(ABC):
    """
    A common interface for Offline Viewer clients (real and dry-run).
    """

    EXPERIMENT_PATHNAME = "metrics_experiment"
    RUN_PATHNAME = "metrics_run"

    @abstractmethod
    def create_experiment(
        self,
        name: Optional[str] = None,
        branch_baseline: Optional[str] = None,
        branch_interest: Optional[str] = None,
        dataset: Optional[str] = None,
        author: Optional[str] = None,
        attributes: Optional[Any] = None,
    ) -> Any:
        raise NotImplementedError

    @abstractmethod
    def create_run(
        self,
        experiment_id: Optional[Union[int, str]] = None,
        commit_hash: Optional[str] = None,
        commit_date: Optional[int] = None,
        attributes: Optional[Any] = None,
    ) -> Any:
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def get_run(self, run_id: Union[int, str]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def get_experiment(self, exp_id: Union[int, str]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def ui_link_prefix(self) -> str:
        """
        The base URL for the UI Offline Viewer (for example,
        https://<INTERNAL_DOMAIN>/offline-viewer).
        """
        raise NotImplementedError

    def exp_link(self, exp_id: str) -> str:
        return urljoin(self.ui_link_prefix(), f"/experiment/{exp_id}")

    def run_link(self, run_id: str) -> str:
        return urljoin(self.ui_link_prefix(), f"/run/{run_id}")
