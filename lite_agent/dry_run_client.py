import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from .task_state import TaskState
from .task_steps_data import TaskStepsData, UPLOAD_ARTIFACTS_TO_SANDBOX_STEP_NAME
from .base_client import BaseLiteAgentClient


class LiteAgentDryRunClient(BaseLiteAgentClient):
    """
    Dry-run client: simulates LiteAgent API.
    The logic of the finalization is tied to the current iteration of the Sandbox:
      - while current_iteration < finalize_on_iteration -> `in_progress`
      - as soon as current_iteration >= finalize_on_iteration -> final status (`_default_status`)

    Task/Step status storage — class (divided by base_url),
    so that different client instances within the same process can see the overall state.
    and at the same time, stable/prestable/unstable did not interfere with each other.
    """

    def __init__(
        self,
        base_url: str,
        default_status: str = "success",
        finalize_on_iteration: int = 2,
        current_iteration: int = 0,
    ):
        self.base_url = base_url
        self._default_status = default_status
        self._finalize_on_iter = int(finalize_on_iteration)
        self._iter = int(current_iteration)
        self._tasks: Dict[str, TaskState] = {}
        self._steps: Dict[str, List[dict]] = {}

    def cancel_task(self, task_id: str) -> bool:
        """
        Immediately mark the task as cancel and enter finish_time.,
        so that on_break() can certainly collect the output (as with the real API).
        """
        key = str(task_id)

        state = self._tasks.get(key)
        now = time.time()
        if not state:
            t_create = datetime.fromtimestamp(now - 2, tz=timezone.utc)
            t_start = datetime.fromtimestamp(now - 1, tz=timezone.utc)
            state = TaskState(
                task_id=key,
                status="in_progress",
                agent_fqdn="dryrun-agent.local",
                api_url=self.base_url,
                creation_time=t_create,
                start_time=t_start,
                finish_time=None,
            )

        finished = TaskState(
            task_id=state.task_id,
            status="cancel",
            agent_fqdn=state.agent_fqdn,
            api_url=state.api_url,
            creation_time=state.creation_time,
            start_time=state.start_time,
            finish_time=datetime.fromtimestamp(now, tz=timezone.utc),
        )
        self._tasks[key] = finished
        if key not in self._steps:
            self._steps[key] = self._build_fake_steps_payload(key)
        return True

    def create_task(self, dict_params: dict) -> TaskState:
        """
        Simulates the creation of a task and puts it in the `in_progress' state.
        Later, `get_task_state()` will move it to the final status according to the iteration threshold.
        """
        now = time.time()
        task_id = f"DRYRUN-{uuid.uuid4()}"
        t_create = datetime.fromtimestamp(now - 2, tz=timezone.utc)
        t_start = datetime.fromtimestamp(now - 1, tz=timezone.utc)
        fqdn = dict_params.get("fqdn") or dict_params.get("agent_fqdn") or "dryrun-agent.local"

        state = TaskState(
            task_id=task_id,
            status="in_progress",
            agent_fqdn=fqdn,
            api_url=self.base_url,
            creation_time=t_create,
            start_time=t_start,
            finish_time=None,
        )
        self._tasks[state.get_task_id()] = state
        return state

    def get_task_state(self, task_id: str) -> TaskState:
        key = str(task_id)
        state = self._tasks.get(key)
        if not state:
            now = time.time()
            state = TaskState(
                task_id=key,
                status="in_progress",
                agent_fqdn="dryrun-agent.local",
                api_url=self.base_url,
                creation_time=datetime.fromtimestamp(now - 2, tz=timezone.utc),
                start_time=datetime.fromtimestamp(now - 1, tz=timezone.utc),
                finish_time=None,
            )
            self._tasks[key] = state

        if state.finish_time is not None or state.status in {"success", "fail", "cancel"}:
            return state

        if self._iter < self._finalize_on_iter:
            return state

        finished = TaskState(
            task_id=state.task_id,
            status=self._default_status,
            agent_fqdn=state.agent_fqdn,
            api_url=state.api_url,
            creation_time=state.creation_time,
            start_time=state.start_time,
            finish_time=datetime.fromtimestamp(time.time(), tz=timezone.utc),
        )
        self._tasks[key] = finished

        if key not in self._steps:
            self._steps[key] = self._build_fake_steps_payload(key)
        return finished

    def get_steps_result(self, task_id: str) -> TaskStepsData:
        key = str(task_id)
        payload = self._steps.get(key)
        if not payload:
            payload = self._build_fake_steps_payload(key)
            self._steps[key] = payload
        return TaskStepsData.from_json(payload)

    def change_agent_availability(self, fqdn: str, availability: str) -> bool:
        return True

    def get_target_daemon_version(self) -> str:
        return "dry-run"

    def _build_fake_steps_payload(self, task_id: str):
        """
        Compatible with TaskStepsData:
        - 'logs' resources for links to step logs;
        - the 'upload-artifacts-to-sandbox' step with 'artifacts' (link only! without id)
            and a direct artifact 'ci_report.html';
        - parameters/statistics/issues.
        """

        def _dur(ms: int) -> str:
            td = timedelta(milliseconds=ms)
            total = int(td.total_seconds() * 1000)
            ms = total % 1000
            sec = (total // 1000) % 60
            minute = (total // (1000 * 60)) % 60
            hour = total // (1000 * 60 * 60)
            return f"{hour:02d}:{minute:02d}:{sec:02d}.{ms:03d}"

        base = self.base_url.rstrip("/")
        return [
            {
                "name": "prepare-environment",
                "status": "success",
                "duration": _dur(850),
                "resources": {
                    "logs": {"link": f"{base}/tasks/{task_id}/steps/prepare-environment/logs"},
                },
                "parameters": {"example.param": "value"},
                "statistics": {"prepare.ms": 850.0},
                "problems": [],
            },
            {
                "name": "run-build",
                "status": "success",
                "duration": _dur(2150),
                "resources": {
                    "logs": {"link": f"{base}/tasks/{task_id}/steps/run-build/logs"},
                },
                "parameters": {"build.target": "DRYRUN"},
                "statistics": {"build.ms": 2150.0},
                "problems": [],
            },
            {
                "name": UPLOAD_ARTIFACTS_TO_SANDBOX_STEP_NAME,
                "status": "success",
                "duration": _dur(420),
                "resources": {
                    "artifacts": {"link": f"https://<INTERNAL_DOMAIN>/task/{task_id}/artifact/artifacts.zip"},
                    "ci_report.html": {
                        "link": f"https://<INTERNAL_DOMAIN>/task/{task_id}/artifact/ci_report.html"
                    },
                    "logs": {"link": f"{base}/tasks/{task_id}/steps/upload-artifacts-to-sandbox/logs"},
                },
                "parameters": {"report.available": "true"},
                "statistics": {"upload.ms": 420.0},
                "problems": [],
            },
        ]

