import sys
import time
import json

from infra.clients.offline_viewer_client import OfflineViewerClient
from infra.clients.offline_viewer_dry_run_client import OfflineViewerDryRunClient
from infra.clients.base_offline_viewer_client import BaseOfflineViewerClient
from sandbox import sdk2
from sandbox.common import errors
from sandbox.common.types import misc as ctm
from sandbox.projects.sdc.common.requests_util import session, log_helper

from sdg.ci.sandbox.utils.poll_frequency_manager.poll_frequency_manager import PollFrequencyManager
from sdg.ci.sandbox.utils.poll_frequency_manager import poll_frequency_profile
from sdg.ci.sandbox.utils.sandbox_button_generator.generator import Generator

SEARCH_URL = "https://<INTERNAL_DOMAIN>/rest/offline_viewer/metrics_experiment_verdict/{exp_id}"
EXPERIMENT_URL = "https://<INTERNAL_DOMAIN>/offline-viewer/experiment/{exp_id}"
SIM_TASK_URL = "https://<INTERNAL_DOMAIN>/task/{task_id}"

TIMEOUT_OUTPUT = {"status": "timeout", "reason": "timeout"}

PROFILE_CHOICES = tuple(poll_frequency_profile.PollProfile.names())


class SdcWaitSimExperiment(sdk2.Task):
    class Requirements(sdk2.Task.Requirements):
        cores = 1
        ram = 1 * 1024  # 1 gb

    class Parameters(sdk2.Task.Parameters):
        with sdk2.parameters.Group("Experiment parameters") as experiment_parameters_block:
            experiment_id = sdk2.parameters.String("Experiment id", required=True)
            publish_verdict = sdk2.parameters.Bool("Publish verdict", default=False)

        with sdk2.parameters.Group("Polling parameters") as polling_parameters_block:
            poll_duration = sdk2.parameters.Integer(
                "Max poll duration seconds (0 - unlimited; works like kill_timeout)", required=False, default=57600
            )
            initial_poll_freq = sdk2.parameters.Integer(
                "Initial poll frequency (seconds)", required=False, default=None
            )
            poll_freq = sdk2.parameters.Integer(
                "Final poll frequency (seconds)", required=False, default=poll_frequency_profile.DEFAULT.final_poll_freq
            )
            transition_duration = sdk2.parameters.Integer(
                "Transition duration from initial to final poll frequency (seconds)",
                required=False,
                default=poll_frequency_profile.DEFAULT.transition_duration,
            )
            poll_freq_profile = sdk2.parameters.String(
                "Poll frequency profile",
                required=False,
                default=poll_frequency_profile.PollProfile.MEDIUM.name,
                choices=PROFILE_CHOICES,
            )

        with sdk2.parameters.Group("Config") as config_block:
            dry_run = sdk2.parameters.Bool("Dry run", default=False)

        with sdk2.parameters.Output:
            experiment_state = sdk2.parameters.String("Experiment state")
            experiment_url_badge = sdk2.parameters.Dict("Experiment url badge")

    def get_exp_state(self):
        if self.Parameters.publish_verdict and self._session is not None:
            response = self._session.get(SEARCH_URL.format(exp_id=self.Parameters.experiment_id))
            log_helper.log_response(response)
            response.raise_for_status()
            response_json = response.json()
        else:
            response_json = self._ov_client.get_experiment(exp_id=self.Parameters.experiment_id)
        return response_json

    def on_prepare(self):
        if self.Parameters.dry_run:
            self._session = None
            self._ov_client: BaseOfflineViewerClient = OfflineViewerDryRunClient(
                finalize_on_iteration=2, current_iteration=self.agentr.iteration
            )
        else:
            self._session = session.create_session()
            self._ov_client: BaseOfflineViewerClient = OfflineViewerClient()

        self.Parameters.experiment_url_badge = self._create_experiment_url_badge(
            module="SDG", url=self.get_experiment_url(), text="Experiment URL", status="SUCCESSFUL"
        )

    def _render_results(self) -> None:
        exp_state = self.get_exp_state()
        self.add_links_block(exp_state)
        self.add_experiment_results_block(exp_state)

    def on_break(self, prev_status, status):
        self._render_results()

    def on_finish(self, prev_status, status):
        self._render_results()

    def add_links_block(self, exp_state: dict) -> None:
        ic_task_ids, ic_task_urls = self.get_ic_task_urls(exp_state)
        links_names = ["Simulator task URL"] + ic_task_ids
        links_values = [self.get_experiment_url()] + ic_task_urls
        title = "Links"
        links_report = Generator(links_names, title, links_values).generate_report()
        self.set_info(links_report, do_escape=False)

    def add_experiment_results_block(self, exp_state: dict) -> None:
        message = f"<h3>Experiment state:</h3><code>{json.dumps(exp_state, indent=4)}</code>"
        self.set_info(message, do_escape=False)

    def get_ic_task_urls(self, exp_state: dict) -> tuple[list[str], list[str]]:
        ic_task_ids: list[str] = []
        metrics_run = exp_state.get("metrics_runs", [])
        for metric_run in metrics_run:
            attributes = metric_run.get("attributes", {})
            ic_task_id = attributes.get("ic_task_id")
            if ic_task_id:
                ic_task_ids.append(ic_task_id)
        ic_task_urls = [SIM_TASK_URL.format(task_id=ic_task_id) for ic_task_id in ic_task_ids]
        return ic_task_ids, ic_task_urls

    def get_experiment_url(self) -> str:
        return EXPERIMENT_URL.format(exp_id=self.Parameters.experiment_id)

    def on_execute(self):
        started_at = self.Context.started_at
        if started_at is ctm.NotExists:
            self.Context.started_at = time.time()

        with self.memoize_stage.poll_stage(sys.maxsize):
            poll_duration = int(self.Parameters.poll_duration)

            if poll_duration > 0:
                elapsed_time = time.time() - self.Context.started_at
                if elapsed_time > poll_duration:
                    self.Context.is_timeout = True
                    self.Parameters.experiment_state = TIMEOUT_OUTPUT
                    raise errors.TaskFailure("Poll duration limit reached (treat as timeout)")

            exp_state = self.get_exp_state()
            status = exp_state.get("status")

            if not status or status in ["enqueued", "pending", "running"]:
                profile = poll_frequency_profile.effective_profile(
                    name=self.Parameters.poll_freq_profile,
                    initial_poll_freq=self.Parameters.initial_poll_freq,
                    poll_freq=int(self.Parameters.poll_freq),
                    transition_duration=int(self.Parameters.transition_duration),
                    tags=self.Parameters.tags,
                )

                elapsed_transition_time = time.time() - self.Context.started_at

                current_poll_freq = PollFrequencyManager.calculate_await_time(
                    elapsed_transition_time,
                    profile.transition_duration,
                    profile.initial_poll_freq,
                    profile.final_poll_freq,
                )

                raise sdk2.WaitTime(current_poll_freq)

            self.Parameters.experiment_state = exp_state
            if status not in ["success", "ready"]:
                raise errors.TaskFailure("Experiment ended with non-success state")

    def _create_experiment_url_badge(self, module: str, url: str, text: str, status: str) -> dict:
        return {"id": "experiment_url_badge", "module": module, "url": url, "text": text, "status": status}
