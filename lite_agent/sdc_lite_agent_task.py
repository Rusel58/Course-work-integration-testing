import re
import sys
import time

import logging


from sandbox import sdk2
from sandbox.common import errors
from sandbox.common.types import misc as ctm
from sandbox.projects.common.task_env import TinyRequirements
from sandbox.projects.common.vcs.arc import Arc
from sandbox.projects.sdc.common import pr_helper
from sandbox.projects.sdc.common.component_handlers.general_component_handler import GeneralComponentHandler
from sandbox.projects.sdc.common.lite_agent_api.client import LiteAgentClient
from sandbox.projects.sdc.common.lite_agent_api.dry_run_client import LiteAgentDryRunClient
from sandbox.projects.sdc.common.lite_agent_api.lite_agent_urls import resolve_base_url, STABLE_URL
from sandbox.projects.sdc.common.lite_agent_api.spawn_task import SpawnTask, ArtifactDirectLink
from sandbox.projects.sdc.common.lite_agent_api.task_state import TaskState
from sandbox.projects.sdc.common.lite_agent_api.task_steps_data import TaskStepsData
from sandbox.projects.sdc.common.sdc_task_report.link_dto import LinkDTO
from sandbox.projects.sdc.common.sdc_task_report.sdc_task_report_helper import SdcTaskReportHelper
from sandbox.projects.sdc.common.support_link_helper import SupportLinkHelper
from sandbox.projects.sdc.common_tasks.EventbusStatisticsMixin import EventbusStatisticsMixin
from sandbox.projects.sdc.common_tasks.base_sdc_task import runtime_parameters_helper
from sandbox.projects.sdc.resource_types import SdcBuildScriptArtifacts

from sdg.ci.common.utils.restart_task_manager.providers.log_providers.file_log_provider import FileLogProvider
from sdg.ci.common.utils.restart_task_manager.restarters.arcadia_ci_restarter import ArcadiaCIRestarter
from sdg.ci.common.utils.restart_task_manager.rules.base_rule import BaseRestartRule
from sdg.ci.common.utils.restart_task_manager.restart_manager_context import RestartManagerContext
from sdg.ci.common.utils.restart_task_manager.restart_task_manager import RestartTaskManager
from sdg.ci.common.utils.restart_task_manager.rules.log_rule import LogRestartRule
from sdg.ci.sandbox.utils.poll_frequency_manager.poll_frequency_manager import PollFrequencyManager
from sandbox.projects.sdc.common.lite_agent_api.base_client import BaseLiteAgentClient
from sdg.ci.sandbox.utils.poll_frequency_manager import poll_frequency_profile

from infra.ci.app.ci_stat_crawler.ch_helper import to_ch_datetime_str

from .lite_agent_resource_helper import LiteAgentResourceHelper

LITE_AGENT_TASK_URL_ORDER = 100

PROFILE_CHOICES = tuple(poll_frequency_profile.PollProfile.names())


class SdcLiteAgentTask(EventbusStatisticsMixin, sdk2.Task):
    SUPPORT_COMPONENT = GeneralComponentHandler()

    class Requirements(TinyRequirements):
        ram = 2 * 1024  # 2GB
        disk_space = 5 * 1024  # 5GB

    # Debug comment
    class Parameters(sdk2.Parameters):
        with sdk2.parameters.Group("Secrets") as secrets_block:
            secret_identifier = sdk2.parameters.YavSecret(
                "YAV secret identifier (with optional version)", default="<REDACTED_SECRET_ID>"
            )
            send_full_token_list_to_lite_agent = sdk2.parameters.Bool(
                "Send full token list to lite-agent daemon", default=True
            )

        with sdk2.parameters.Group("Polling parameters") as polling_parameters_block:
            initial_poll_freq = sdk2.parameters.Integer(
                "Initial poll frequency (seconds)", required=False, default=None
            )
            transition_duration = sdk2.parameters.Integer(
                "Transition duration from initial to final poll frequency (seconds)",
                required=False,
                default=poll_frequency_profile.DEFAULT.transition_duration,
            )
            poll_freq = sdk2.parameters.Integer(
                "Final poll frequency (seconds)", required=False, default=poll_frequency_profile.DEFAULT.final_poll_freq
            )
            poll_freq_profile = sdk2.parameters.String(
                "Poll frequency profile",
                required=False,
                default=None,
                choices=PROFILE_CHOICES,
            )
            poll_duration = sdk2.parameters.Integer(
                "Max poll duration seconds(0 - unlimited; works like kill_timeout)", required=False, default=0
            )
            wait_for_cancel = sdk2.parameters.Integer(
                "Time to wait cancel of underlying build after poll duration exceed", default=300
            )

        with sdk2.parameters.Group("Config") as config_block:
            api_type = sdk2.parameters.String("LiteAgent api type", default="stable")
            auto_cancel = sdk2.parameters.Bool(
                "Auto cancel underlying teamcity build if needed", required=False, default=True
            )
            existing_task_id = sdk2.parameters.String("Use existing task_id [skip spawn]", required=False)
            branch = sdk2.parameters.String("LiteAgent spawn branch to spawn", required=True)
            commit = sdk2.parameters.String("LiteAgent spawn commit to spawn")
            arc_vcs_project_dir = sdk2.parameters.String("Arc VCS project-dir", default="sdg/sdc")

        with sdk2.parameters.Group("LiteAgent parameters") as lite_agent_parameters_block:
            task_type = sdk2.parameters.String("task_type", required=True)
            agent_tags = sdk2.parameters.List("agent_tags", required=True)
            agent_fqdn = sdk2.parameters.String("agent_fqdn", default="")
            maintenance = sdk2.parameters.Bool("maintenance", default=False)

        with sdk2.parameters.Group("Artifacts and logs") as artifacts_logs_parameters_block:
            ttl_artifacts_zip = sdk2.parameters.Integer("TTL for artifacts.zip", default=7)
            ttl_logs = sdk2.parameters.Integer("TTL for logs", default=14)
            file_artifact_type = sdk2.parameters.String(
                "Type of sandbox resource for separate file", default="SDC_BUILD_SCRIPT_FILE_ARTIFACT"
            )
            artifact_zip_type = sdk2.parameters.String(
                "Type of sandbox resource for artifacts.zip", default="SDC_BUILD_SCRIPT_ARTIFACTS"
            )

        # Resulting parameters
        with sdk2.parameters.Output(reset_on_restart=True):
            out_task_id = sdk2.parameters.String("Resulting lite agent task id")
            out_task_url = sdk2.parameters.String("Resulting lite agent task url")
            out_task_status = sdk2.parameters.String("Resulting lite agent task status")
            out_agent_fqdn = sdk2.parameters.String("Resulting agent fqdn")
            out_artifacts_zip = sdk2.parameters.Resource(
                "Artifacts zip resource",
                resource_type=SdcBuildScriptArtifacts,
                required=False,
            )

            runtime_parameters = sdk2.parameters.JSON("Collected runtime parameters")
            runtime_statistics = sdk2.parameters.Dict("Collected runtime statistics")
            runtime_build_problems = sdk2.parameters.JSON("Collected runtime build problems")

            lite_agent_step_resource_ids = sdk2.parameters.List("Step resource ids")

            _vcs_info = sdk2.parameters.Dict("VCS info")

    @property
    def commit(self):
        return self.Parameters.commit

    @property
    def branch(self):
        return self.Parameters.branch

    @property
    def is_timeout(self):
        return self.Context.is_timeout is not ctm.NotExists

    @property
    def pr_id(self):
        return pr_helper.pr_id_from_branch_name(self.branch)

    @property
    def arcadia_ci_context(self):
        return self.Context.__values__.get("__CI_CONTEXT", ctm.NotExists)

    @property
    def is_on_arcadia_ci(self):
        return self.arcadia_ci_context is not ctm.NotExists

    @property
    def task_url(self):
        return "https://<INTERNAL_DOMAIN>/task/{}".format(self.id)

    @property
    def ttl_logs(self):
        return int(self.Parameters.ttl_logs)

    @property
    def files_with_direct_link(self):
        return [
            self.artifact_with_task_info(
                "ci_report.html",
                "ci_report.html",
                self.Parameters.ttl_logs,
                self.Parameters.file_artifact_type,
                "report with information about build problems",
            ),
        ]

    @property
    def artifact_zip_params(self):
        return self.artifact_with_task_info(
            "artifacts",
            "artifacts",
            self.Parameters.ttl_artifacts_zip,
            self.Parameters.artifact_zip_type,
            "archive with all created artifacts",
        )

    def artifact_with_task_info(self, name, wildcard, ttl, resource_type, description):
        return ArtifactDirectLink(
            name,
            wildcard,
            ttl,
            resource_type,
            description,
            {"name": name, "task_type": str(self.type), "task_id": str(self.id)},
        )

    def resolve_commit(self, branch):
        tokens = self.Parameters.secret_identifier.data()
        arc_token = tokens["token.arc"]

        arc = Arc(arc_oauth_token=arc_token)
        with arc.mount_path(path=None, changeset=None) as mount_point:
            commit = arc.log(
                mount_point=mount_point,
                branch=branch,
                first_parent=True,
                max_count=1,
                path=str(self.Parameters.arc_vcs_project_dir),
                as_dict=True,
            )[0]
            return commit["commit"]

    def create_api_client(self):
        """
        :type api: LiteAgentClient
        """
        tokens = self.Parameters.secret_identifier.data()
        api_token = tokens["token.lite_agent_api"]
        api_type = str(self.Parameters.api_type).strip().lower()

        if api_type == "dry-run":
            return LiteAgentDryRunClient(
                base_url=STABLE_URL,
                current_iteration=self.agentr.iteration,
                finalize_on_iteration=2,
            )

        base_url = resolve_base_url(api_type)
        return LiteAgentClient(base_url=base_url, token=api_token)

    def cancel_underlying_task(self):
        # Task was restarted. because we have failed task.
        if RestartManagerContext(self).is_task_restarted():
            return

        task_id = self.Context.lite_agent_task_id
        if task_id == ctm.NotExists:
            return

        if not self.Parameters.auto_cancel:
            return

        api = self.create_api_client()
        api.cancel_task(task_id)

    def on_break(self, prev_status, status):
        super(SdcLiteAgentTask, self).on_break(prev_status, status)
        self.cancel_underlying_task()
        la_task_id = self.Context.lite_agent_task_id
        if not la_task_id:
            return
        api = self.create_api_client()
        self.setup_output(la_task_id, api)

    def get_links(self, task_info: TaskState, task_steps_result: TaskStepsData):
        links = []

        # Add common links
        links.extend(
            [
                LinkDTO("Artifacts.zip", task_steps_result.get_artifacts_zip_url(), order=0),
                LinkDTO("LiteAgent TaskURL", task_info.get_task_url(), order=LITE_AGENT_TASK_URL_ORDER),
            ]
        )

        # Add direct artifact links
        for step_resource_link in task_steps_result.get_artifacts_with_direct_link():
            link_name = step_resource_link.resource_name
            link_value = step_resource_link.url
            links.append(LinkDTO(link_name, link_value, order=1))

        # Add business-logic links (reported from runtime script)
        sdc_links = runtime_parameters_helper.grab_sdc_links(task_steps_result.get_runtime_parameters())
        for sl in sdc_links:
            sl.order = 2

        links.extend(sdc_links)

        # Add lite-agent step-log links
        all_steps_logs = task_steps_result.get_step_log_links()

        for i, step_log in enumerate(all_steps_logs):
            link_dto = LinkDTO(
                placeholder="[System step log] {}. {} (duration: {})".format(
                    i + 1, step_log.step_name, re.sub(r"\.[0-9]*", "", step_log.step_duration)
                ),
                url=step_log.url,
                order=LITE_AGENT_TASK_URL_ORDER + i + 1,
                color="red" if step_log.from_failed_step else None,
            )
            links.append(link_dto)

        # Filter nullable links
        links = [link for link in links if link.url]
        return links

    def update_task_info(self, task_info: TaskState, task_steps_result: TaskStepsData, max_problems=15):
        if not task_info:
            return

        links = self.get_links(task_info, task_steps_result)

        all_problems = task_steps_result.get_build_problems_text_only()
        all_problems = all_problems[:max_problems]

        support_links = SupportLinkHelper(
            support_component=self.SUPPORT_COMPONENT,
            task_id=self.id,
            title=self.type,
            pr_id=self.pr_id,
            build_problems=all_problems,
        ).get_support_links()
        self.Parameters.tags += self.SUPPORT_COMPONENT.tags

        report_helper = SdcTaskReportHelper(build_problems=all_problems, links=links, support_links=support_links)
        task_info = report_helper.get_task_info()
        if not task_info:
            return

        self.set_info(task_info, do_escape=False)

    def report_spawned_build_url(self):
        build_url = self.Context.lite_agent_task_url
        if build_url is ctm.NotExists:
            return

        with self.memoize_stage.report_spawned_build():
            link = LinkDTO(
                placeholder="Spawned task url", url=build_url, css_class="yc-link_view_normal log-links__link"
            )
            self.set_info(link.to_html(), do_escape=False)

    def on_execute(self):
        merge_pin_pattern = re.compile(r".*/tier1/.*/merge_pin$")
        if merge_pin_pattern.match(str(self.Parameters.branch)):
            raise ValueError(
                "the use of '*/merge_pin' branches is not supported"
                "more detailed https://<INTERNAL_DOMAIN>/sdc/infra/building-project/ci-faq/#usersbranch"
            )

        started_at = self.Context.started_at
        if started_at is ctm.NotExists:
            self.Context.started_at = time.time()

        api = self.create_api_client()

        with self.memoize_stage.spawn_stage(commit_on_entrance=False):
            self.do_spawn_stage(api)

        with self.memoize_stage.poll_stage(sys.maxsize) as st:
            run_no = st.runs

            la_task_id = self.Context.lite_agent_task_id

            task_info = api.get_task_state(la_task_id)
            task_url = task_info.get_task_url()
            task_state = task_info.get_status()

            self.Context.lite_agent_task_status = task_state
            self.Context.lite_agent_task_url = task_url

            # handle case when spawn stage is skipped (Parameter existing_task_id is set)
            self.report_spawned_build_url()

            logging.info("Build: %s state: %s PollNo: %s", task_url, task_state, run_no)

            if task_info.in_progress() and not self.is_timeout:
                # Handle poll_duration
                poll_duration = int(self.Parameters.poll_duration)

                if poll_duration > 0:
                    elapsed_time = time.time() - self.Context.started_at
                    if elapsed_time > poll_duration:
                        self.Context.is_timeout = True
                        self.cancel_underlying_task()
                        raise sdk2.WaitTime(self.Parameters.wait_for_cancel)

                if str(self.Parameters.api_type).strip().lower() == "dry-run":
                    profile = poll_frequency_profile.PollProfile.DRY_RUN.value
                else:
                    profile = poll_frequency_profile.effective_profile(
                        name=self.Parameters.poll_freq_profile,
                        initial_poll_freq=self.Parameters.initial_poll_freq,
                        poll_freq=int(self.Parameters.poll_freq),
                        transition_duration=int(self.Parameters.transition_duration),
                        tags=self.Parameters.tags,
                    )

                final_poll_freq = profile.final_poll_freq
                transition_duration = profile.transition_duration
                initial_poll_freq = profile.initial_poll_freq

                elapsed_transition_time = time.time() - self.Context.started_at

                current_poll_freq = PollFrequencyManager.calculate_await_time(
                    elapsed_transition_time,
                    transition_duration,
                    initial_poll_freq,
                    final_poll_freq,
                )
                raise sdk2.WaitTime(current_poll_freq)

            # TODO: RETRY HANDLE
            logging.info("Build %s finished, status: %s", la_task_id, task_state)

            self.setup_output(la_task_id, api)

            if self.is_timeout:
                raise errors.TaskFailure("Poll duration limit reached(treat as timeout)")

            if not task_info.is_success():
                restart_task_manager = self.get_restart_task_manager()
                restart_task_manager.restart_if_needed()
                raise errors.TaskFailure("Lite agent task failed")

    def get_restarter(self):
        return ArcadiaCIRestarter(self)

    def get_max_restarts(self) -> int:
        return 0

    def get_restart_task_manager(self) -> RestartTaskManager:
        return RestartTaskManager(self, self.get_restart_rules(), self.get_restarter(), self.get_max_restarts())

    def get_restart_rules(self) -> list[BaseRestartRule]:
        return []

    def setup_output(self, la_task_id: str, api: BaseLiteAgentClient):
        task_info = api.get_task_state(la_task_id)
        task_steps_result = api.get_steps_result(la_task_id)
        self.setup_out_parameters(task_info, task_steps_result)
        self.update_task_info(task_info, task_steps_result)

    def get_extra_runtime_parameters(self, task_info):
        extra_runtime_parameters = {
            "sdc_ci_metadata.lite_agent_task_creation_time": to_ch_datetime_str(task_info.get_creation_time()),
            "sdc_ci_metadata.lite_agent_task_start_time": to_ch_datetime_str(task_info.get_start_time()),
            "sdc_ci_metadata.lite_agent_task_finish_time": to_ch_datetime_str(task_info.get_finish_time()),
            "sdc_ci_metadata.sandbox_task_iteration": self.agentr.iteration,
        }
        agent_fqdn = task_info.get_agent_fqdn()
        if agent_fqdn:
            extra_runtime_parameters["agent.hostname"] = agent_fqdn
        return extra_runtime_parameters

    def setup_out_parameters(self, task_info: TaskState, task_steps_result: TaskStepsData):
        self.Parameters.out_task_id = task_info.get_task_id()
        self.Parameters.out_task_url = task_info.get_task_url()
        self.Parameters.out_task_status = task_info.get_status()
        self.Parameters.out_agent_fqdn = task_info.get_agent_fqdn()
        artifacts_resource_id = task_steps_result.get_artifacts_zip_id()
        if artifacts_resource_id:
            self.Parameters.out_artifacts_zip = sdk2.Resource[artifacts_resource_id]
        runtime_parameters = task_steps_result.get_runtime_parameters()
        runtime_parameters.update(self.get_extra_runtime_parameters(task_info))
        self.Parameters.runtime_parameters = runtime_parameters
        self.Parameters.runtime_statistics = task_steps_result.get_runtime_statistics()
        self.Parameters.lite_agent_step_resource_ids = task_steps_result.get_all_resource_ids_from_steps()

        self.Parameters.runtime_build_problems = [
            # Same output format like in BaseSdcTask:
            # https://<INTERNAL_DOMAIN>/arcadia/sandbox/projects/sdc/common_tasks/base_sdc_task/BaseSdcTask.py?rev=r13416885#L493
            {"message": bp.description, "identity": bp.identity}
            for bp in task_steps_result.get_build_problems()
        ]

    def do_spawn_stage(self, api):
        """
        :type api: LiteAgentClient
        """

        # Use existing task or spawn new
        task_id = self.Parameters.existing_task_id

        if task_id:
            logging.info("Use existing build id: %s [skip spawn stage]", task_id)
            self.Context.lite_agent_task_id = task_id
            return  # poll now! do not enter WaitTime stage

        commit = self.Parameters.commit or self.resolve_commit(self.Parameters.branch)

        dto = SpawnTask.create(
            task_type=self.Parameters.task_type,
            arc_vcs_project_dir=str(self.Parameters.arc_vcs_project_dir),
            branch=str(self.Parameters.branch),
            commit=commit,
            env_variables=self.get_env_variables(),
            tag_filters=list(self.Parameters.agent_tags),
            fqdn=self.Parameters.agent_fqdn,
            files_with_direct_link=self.files_with_direct_link,
            artifact_zip_params=self.artifact_zip_params,
            ttl_logs=self.ttl_logs,
            maintenance=self.Parameters.maintenance,
        )
        task_state = api.create_task(dto.to_dict())

        task_id = task_state.get_task_id()

        self.Context.lite_agent_task_url = task_state.get_task_url()
        self.Context.lite_agent_task_id = task_id

        self.report_spawned_build_url()

        if str(self.Parameters.api_type).strip().lower() == "dry-run":
            profile = poll_frequency_profile.PollProfile.DRY_RUN.value
        else:
            profile = poll_frequency_profile.effective_profile(
                name=self.Parameters.poll_freq_profile,
                initial_poll_freq=self.Parameters.initial_poll_freq,
                poll_freq=int(self.Parameters.poll_freq),
                transition_duration=int(self.Parameters.transition_duration),
                tags=self.Parameters.tags,
            )
        initial_poll_freq = profile.initial_poll_freq

        raise sdk2.WaitTime(initial_poll_freq)

    # TODO: reuse same list like in BaseSdcTask
    def get_env_variables(self):
        env_vars = {
            "T__TEAMCITY_BUILD_TYPE_ID": str(self.type),
            "T__TEAMCITY_BUILD_LOGICAL_TYPE": str(self.type),
            "T__TEAMCITY_BUILD_BRANCH": self.Parameters.branch,
            "T__TEAMCITY_BUILD_COMMIT": self.Parameters.commit,
            "T__PARENT_BUILD_ID": str(self.id),
            "T__PARENT_BUILD_TYPE_ID": str(self.type),
            "T__TEAMCITY_BUILD_TYPE_NAME": str(self.type),
            "T__PARENT_BUILD_URL": "https://<INTERNAL_DOMAIN>/task/{}/".format(self.id),
            "TEAMCITY_VERSION": "LITE_AGENT",
            "T__BUILD_TYPE": "RELEASE",
            "VCS_TYPE": "arc",
        }
        if not self.Parameters.send_full_token_list_to_lite_agent:
            return env_vars

        env_vars.update(
            {
                # Secrets is not working right now
                "T__BB_TOKEN": self.secret_identifier_secret_env("token.bb"),
                "T__CH_SDC_MON_PASS": self.secret_identifier_secret_env("clickhouse.sdc_mon.password"),
                "T__SECRET__ARC_TOKEN": self.secret_identifier_secret_env("token.arc"),
                "T__SECRET__ABC_TOKEN": self.secret_identifier_secret_env("token.staff"),
                "T__SECRET__BB_TOKEN": self.secret_identifier_secret_env("token.bb"),
                "T__SECRET__LOGBROKER_OAUTH_TOKEN": self.secret_identifier_secret_env("token.logbroker"),
                "T__SECRET__NIRVANA_TOKEN": self.secret_identifier_secret_env("token.nirvana"),
                "T__SECRET__SOLOMON_TOKEN": self.secret_identifier_secret_env("token.solomon"),
                "T__SECRET__ST_TOKEN": self.secret_identifier_secret_env("token.startrek"),
                "T__SECRET__STAFF_TOKEN": self.secret_identifier_secret_env("token.staff"),
                "T__SECRET__TEAMCITY_TOKEN": self.secret_identifier_secret_env("token.teamcity"),
                "T__SECRET__TVM_SDC_CI_TOKEN": self.secret_identifier_secret_env("token.tvm_sdc_ci"),
                "T__SECRET__SANDBOX_TOKEN": self.secret_identifier_secret_env("token.sandbox"),
                "T__SECRET__YQL_TOKEN": self.secret_identifier_secret_env("token.yql"),
                "T__SECRET__REACTOR_TOKEN": self.secret_identifier_secret_env("token.nirvana"),
                "YT_TOKEN": self.secret_identifier_secret_env("token.yt"),
                # For now: any valid OAUTH2 token is required
                "T__SECRET__ISOLATE_CLOUD_TOKEN": self.secret_identifier_secret_env("token.teamcity"),
            }
        )
        return env_vars

    def secret_identifier_secret_env(self, key):
        return self.yav_secret_env(str(self.Parameters.secret_identifier), key)

    def yav_secret_env(self, yav_secret_id, vault_key):
        # Example: yav@<REDACTED_SECRET_ID>@tokens.sdc-bitbucket-oauth-password
        return "#yav@{}@{}".format(yav_secret_id, vault_key)

    def create_lite_agent_log_rule(
        self,
        la_api: BaseLiteAgentClient,
        la_task_id: str,
        step_name: str,
        patterns_to_search: list[str],
        alias_error: str,
    ) -> LogRestartRule:
        resource_path = LiteAgentResourceHelper(la_api, la_task_id, step_name).get_log_path()
        log_provider = FileLogProvider(resource_path)

        return LogRestartRule(self, log_provider, patterns_to_search, alias_error)

