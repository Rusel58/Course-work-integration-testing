# coding=utf-8
import json
import logging
import re
import sys
import time
import uuid
import html

import yaml

from sandbox import sdk2
from sandbox.common import errors
from sandbox.common.types import misc as ctm
from sdg.ci.sandbox.utils.poll_frequency_manager.poll_frequency_manager import PollFrequencyManager
from sdg.ci.sandbox.utils.poll_frequency_manager import poll_frequency_profile

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

SMART_BOTS_NIRVANA_SECRET_ID = "<REDACTED>"
PROFILE_CHOICES = tuple(poll_frequency_profile.PollProfile.names())

_CI_JOB_RE = re.compile(r"(?im)^\s*CI\s*job\s*:\s*(.+?)\s*$")
_CI_LAUNCH_RE = re.compile(r"(?im)^\s*CI\s*launch\s*:\s*(.+?)\s*$")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_A_TAG_RE = re.compile(r'(?is)<a\b[^>]*href="([^"]+)"[^>]*>(.*?)</a>')
_BR_RE = re.compile(r"(?i)<br\s*/?>")


class NirvanaClient(object):
    def __init__(self, oauth_token):
        self.url = "https://<INTERNAL_DOMAIN>/api/public/v1/"
        self.oauth_token = oauth_token
        self.session = requests.Session()
        self.session.headers["Authorization"] = "OAuth {}".format(self.oauth_token)
        self.session.headers["Content-Type"] = "application/json"
        retry = Retry(
            total=5,
            backoff_factor=0.3,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def download_resource(self, url):
        response = self.session.get(url)
        response.raise_for_status()
        try:
            return response.json()
        except (json.JSONDecodeError, ValueError):
            return response.text

    def make_request(self, url, params):
        logger.debug("Making request to {}. Params: {}".format(url, params))
        request_id = str(uuid.uuid4())
        jsondata = json.dumps({"jsonrpc": "2.0", "method": url, "id": request_id, "params": params})
        response = self.session.post(self.url + url, data=jsondata, verify=False)
        response.raise_for_status()
        response_content = response.json()
        logger.debug("Result: {}".format(response_content))
        if "result" not in response_content:
            if "error" in response_content and "message" in response_content["error"]:
                raise Exception(str(response_content["error"]["message"]))
            else:
                raise Exception("Unknown exception")
        return response_content["result"]


class DryRunNirvanaClient(object):
    def __init__(self, iteration):
        self.url = "https://<INTERNAL_DOMAIN>/api/public/v1/"
        self.iteration = iteration
        self.mock_data = {
            "cloneWorkflowInstance": "dry_run_instance_id",
            "setGlobalParameters": True,
            "startWorkflow": True,
            "getExecutionState": {"status": "not completed", "result": "success"},
            "getWorkflowResults": {
                "results": [
                    {
                        "endpoint": "experiment_url",
                        "directStoragePath": "dry_run_experiment_url",
                    },
                    {
                        "endpoint": "baseline_exec_info",
                        "directStoragePath": "dry_run_baseline_exec_info",
                    },
                    {
                        "endpoint": "result",
                        "directStoragePath": "dry_run_result",
                    },
                    {
                        "endpoint": "competitor_exec_info",
                        "directStoragePath": "dry_run_competitor_exec_info",
                    },
                    {
                        "endpoint": "dashboard_link",
                        "directStoragePath": "dry_run_dashboard_link",
                    },
                ]
            },
            "getWorkflowSummary": {"blockSummaries": [{"innerWorkflowInstanceId": "dry_run_instance_id"}]},
            "getBlockResults": [
                {
                    "results": [
                        {
                            "endpoint": "link",
                            "directStoragePath": "dry_run_ov_link",
                        }
                    ]
                }
            ],
            "stopWorkflow": True,
            "addCommentToWorkflowInstance": True,
        }

    def download_resource(self, url):
        if url == "dry_run_result":
            return "passed"

        if url == "dry_run_experiment_url":
            return "https://<INTERNAL_DOMAIN>/offline-viewer/experiment/dry-run-experiment"

        if url == "dry_run_baseline_exec_info":
            return {
                "cluster": "dry-run-cluster",
                "table": "//home/selfdriving/simulator/ci/dry-run/baseline",
            }

        if url == "dry_run_competitor_exec_info":
            return {
                "cluster": "dry-run-cluster",
                "table": "//home/selfdriving/simulator/ci/dry-run/competitor",
            }

        if url == "dry_run_dashboard_link":
            return [{"dashboard_link": "https://<INTERNAL_DOMAIN>/dry-run-dashboard"}]

        if url == "dry_run_ov_link":
            return "https://<INTERNAL_DOMAIN>/dry-run-ov-exp"

        return {}

    def make_request(self, url, params):
        if url in self.mock_data:
            if url == "getExecutionState" and self.iteration > 1:
                self.mock_data[url]["status"] = "completed"
            return self.mock_data[url]
        else:
            raise Exception(f"No mock data available for URL: {url}")


class SdcRunNirvanaWorkflow(sdk2.Task):
    class Requirements(sdk2.Task.Requirements):
        cores = 1
        ram = 4 * 1024

    class Parameters(sdk2.Task.Parameters):
        with sdk2.parameters.Group("Secrets") as secrets_block:
            nirvana_token = sdk2.parameters.YavSecret(
                "YAV secret identifier (with optional version)", default=SMART_BOTS_NIRVANA_SECRET_ID
            )
            nirvana_token_with_key = sdk2.parameters.YavSecretWithKey("Nirvana token with the key")

        with sdk2.parameters.Group("Nirvana parameters") as nirvana_parameters_block:
            nirvana_quota = sdk2.parameters.String("Nirvana quota", default_value="default", required=True)

            nirvana_workflow_id = sdk2.parameters.String(
                "Template Nirvana workflow id",
                description="New workflow will be instantiated from this one",
                required=True,
            )
            nirvana_workflow_instance_id = sdk2.parameters.String(
                "Template Nirvana workflow instance id",
                description="Specific instance ID to clone from",
                default=None,
            )
            nirvana_global_options = sdk2.parameters.Dict("Nirvana workflow global options")
            existing_workflow_instance_id = sdk2.parameters.String("Existing workflow instance id", default=None)
            stop_flow_on_terminate = sdk2.parameters.Bool("Stop Nirvana workflow on task termination")
            clone_to_new_workflow = sdk2.parameters.Bool(
                "Clone Nirvana workflow or not",
                description="Clone to new workflow instead of new instance in the same workflow",
                default=True,
            )
            publish_nirvana_output = sdk2.parameters.Bool(
                "Publish nirvana workflow results as output parameters", default=False
            )
            process_result = sdk2.parameters.Bool("Should process result", default=False)
            with clone_to_new_workflow.value[True]:
                nirvana_project_id = sdk2.parameters.String(
                    "Project id", description="Will be given to the newly cloned workflow", required=False
                )
                nirvana_workflow_name = sdk2.parameters.String(
                    "Workflow name",
                    default_value="Sandbox driven workflow",
                    description="Will be given to the newly cloned workflow",
                    required=False,
                )
                target_workflow = sdk2.parameters.String(
                    "Workflow id, in which new instance will be started", default=None, required=False
                )

        with sdk2.parameters.Group("Config") as config_block:
            wait_workflow_end = sdk2.parameters.Bool("Wait workflow end", default=True)
            dry_run = sdk2.parameters.Bool("Dry run", default=False)

        with sdk2.parameters.Group("Polling parameters") as polling_parameters_block:
            poll_duration = sdk2.parameters.Integer(
                "Max poll duration seconds (0 - unlimited; works like kill_timeout)", required=False, default=0
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
                default=None,
                choices=PROFILE_CHOICES,
            )

        with sdk2.parameters.Output(reset_on_restart=True):
            completion_status = sdk2.parameters.String("Task completion status")
            executed_workflow_instance_id = sdk2.parameters.String("Running workflow instance")
            executed_workflow_id = sdk2.parameters.String("Executed workflow id")
            executed_workflow_url = sdk2.parameters.Url("Executed workflow")
            nirvana_results = sdk2.parameters.JSON("Nirvana workflow output")
            runtime_parameters = sdk2.parameters.Dict("Collected runtime parameters")
            executed_workflow_badge = sdk2.parameters.Dict("Executed workflow badge")

    def _read_nirvana_token_from_yav(self) -> str:
        nirvana_token_with_key = self.Parameters.nirvana_token_with_key
        if nirvana_token_with_key is not None:
            return self.Parameters.nirvana_token_with_key.value()
        return self.Parameters.nirvana_token.data()["secret"]

    def get_nirvana_client(self):
        nv_token = self._read_nirvana_token_from_yav()
        if self.Parameters.dry_run:
            return DryRunNirvanaClient(self.agentr.iteration)
        return NirvanaClient(oauth_token=nv_token)

    @staticmethod
    def build_sandbox_task_url(task_id: int) -> str:
        return f"https://<INTERNAL_DOMAIN>/task/{task_id}"

    @staticmethod
    def _html_to_text(text: str) -> str:
        raw = text or ""
        raw = _BR_RE.sub("\n", raw)

        def _a_repl(m: re.Match) -> str:
            href = html.unescape(m.group(1)).strip()
            inner_html = m.group(2) or ""
            inner_txt = _HTML_TAG_RE.sub("", inner_html)
            inner_txt = html.unescape(inner_txt).strip()
            if inner_txt and inner_txt != href:
                return f"{inner_txt} ({href})"
            return href

        raw = _A_TAG_RE.sub(_a_repl, raw)
        raw = _HTML_TAG_RE.sub("", raw)

        return html.unescape(raw).strip()

    @staticmethod
    def build_workflow_url(workflow_id, workflow_instance_id=None):
        if workflow_instance_id and workflow_id:
            return "https://<INTERNAL_DOMAIN>/flow/{}/{}/graph".format(workflow_id, workflow_instance_id)
        if workflow_id:
            return "https://<INTERNAL_DOMAIN>/flow/{}/graph".format(workflow_id)

        return None

    def process_result(self, nirvana_results):
        pass

    def get_metadata_values(self) -> dict[str, str]:
        params = {
            "child_process_url": self.Parameters.executed_workflow_url or "",
            "sandbox_task_iteration": self.agentr.iteration,
        }

        return params

    def update_metadata(self) -> None:
        params = self.get_metadata_values()
        self.Parameters.runtime_parameters = {"sdc_ci_metadata.{}".format(k): v for k, v in params.items()}

    def on_workflow_failed(self, execution_result, completion_status):
        raise errors.TaskFailure(completion_status)

    def do_spawn_stage(self):
        if self.Parameters.executed_workflow_instance_id:
            logger.info("Workflow already executed: %s", self.Parameters.executed_workflow_url)
            return

        template_workflow_id = self.Parameters.nirvana_workflow_id
        # hack to process empty string as None
        template_workflow_instance_id = self.Parameters.nirvana_workflow_instance_id or None
        template_workflow_url = SdcRunNirvanaWorkflow.build_workflow_url(
            template_workflow_id,
            template_workflow_instance_id,
        )
        logger.info("Template workflow: %s", template_workflow_url)

        nirvana_workflow_name = self.Parameters.nirvana_workflow_name
        nirvana_project_id = self.Parameters.nirvana_project_id
        clone_to_new_workflow = self.Parameters.clone_to_new_workflow

        executed_workflow_instance_id = self.Parameters.existing_workflow_instance_id
        executed_workflow_id = self.Parameters.target_workflow or template_workflow_id
        if not executed_workflow_instance_id:
            client = self.get_nirvana_client()
            if clone_to_new_workflow:
                executed_workflow_instance_id = client.make_request(
                    "cloneWorkflowInstance",
                    dict(
                        workflowId=template_workflow_id,
                        workflowInstanceId=template_workflow_instance_id,
                        newName=nirvana_workflow_name,
                        targetWorkflowId=self.Parameters.target_workflow,
                        newProjectCode=nirvana_project_id if nirvana_project_id else None,
                        newQuotaProjectId=self.Parameters.nirvana_quota,
                    ),
                )
            else:
                executed_workflow_instance_id = client.make_request(
                    "cloneWorkflowInstance",
                    dict(
                        workflowId=template_workflow_id,
                        workflowInstanceId=template_workflow_instance_id,
                        newQuotaProjectId=self.Parameters.nirvana_quota,
                    ),
                )

            global_options = self.Parameters.nirvana_global_options
            if global_options:
                client.make_request(
                    "setGlobalParameters",
                    dict(
                        workflowId=executed_workflow_id,
                        workflowInstanceId=executed_workflow_instance_id,
                        params=[
                            dict(parameter=key, value=yaml.safe_load(value)) for key, value in global_options.items()
                        ],
                    ),
                )

            client.make_request(
                "startWorkflow", dict(workflowId=executed_workflow_id, workflowInstanceId=executed_workflow_instance_id)
            )

            comment = self.build_workflow_instance_comment()
            client.make_request(
                "addCommentToWorkflowInstance",
                dict(workflowInstanceId=executed_workflow_instance_id, comment=comment),
            )
            logger.info("Added workflow instance comment to %s", executed_workflow_instance_id)

        exec_workflow_url = SdcRunNirvanaWorkflow.build_workflow_url(
            workflow_id=executed_workflow_id, workflow_instance_id=executed_workflow_instance_id
        )

        self.Parameters.executed_workflow_instance_id = executed_workflow_instance_id
        self.Parameters.executed_workflow_url = exec_workflow_url
        self.Parameters.executed_workflow_id = executed_workflow_id
        self.Parameters.executed_workflow_badge = self.create_badge(
            badge_id="executed_workflow_badge",
            module="NIRVANA",
            url=self.Parameters.executed_workflow_url,
            text="Executed workfow",
            status="SUCCESSFUL",
        )

    def on_execution_tick(self):
        pass

    def on_execute(self):
        started_at = self.Context.started_at
        if started_at is ctm.NotExists:
            self.Context.started_at = time.time()

        client = self.get_nirvana_client()

        with self.memoize_stage.spawn_stage(commit_on_entrance=False):
            self.do_spawn_stage()

        with self.memoize_stage.poll_stage(sys.maxsize):
            exec_workflow_url = self.Parameters.executed_workflow_url
            executed_workflow_id = self.Parameters.executed_workflow_id
            executed_workflow_instance_id = self.Parameters.executed_workflow_instance_id
            need_to_fail = False

            if self.Parameters.dry_run:
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

            while self.Parameters.wait_workflow_end:
                poll_duration = int(self.Parameters.poll_duration)
                if poll_duration > 0:
                    elapsed_time = time.time() - self.Context.started_at
                    if elapsed_time > poll_duration:
                        self.on_workflow_timeout()
                        need_to_fail = True
                        execution_result = "timeout"
                        break

                try:
                    # https://<INTERNAL_DOMAIN>/nirvana/components/api/#getexecutionstatestatusvypolnenijaworkflow
                    get_execution_state_args = {}
                    if executed_workflow_instance_id:
                        get_execution_state_args["workflowInstanceId"] = executed_workflow_instance_id
                    else:
                        get_execution_state_args["workflowId"] = executed_workflow_id
                    progress = dict(client.make_request("getExecutionState", get_execution_state_args))
                    logger.info("Workflow %s progress info: %s", exec_workflow_url, progress)
                    if progress["status"] == "completed":
                        execution_result = progress["result"]
                        if execution_result != "success":
                            logger.info("Workflow completed with no success (result: %s)", execution_result)
                            need_to_fail = True
                        break
                except Exception as exc:
                    logger.warning("Failed to execute GetExecutionState: %s", exc)

                self.on_execution_tick()

                elapsed_transition_time = time.time() - self.Context.started_at

                current_poll_freq = PollFrequencyManager.calculate_await_time(
                    elapsed_transition_time,
                    transition_duration,
                    initial_poll_freq,
                    final_poll_freq,
                )

                logger.debug(f"current_poll_freq: {current_poll_freq}")
                raise sdk2.WaitTime(current_poll_freq)

            if need_to_fail:
                if execution_result:
                    self.Parameters.completion_status = "Workflow has been ended with status {}. See {}".format(
                        execution_result,
                        exec_workflow_url,
                    )
                    raise errors.TaskFailure(self.Parameters.completion_status)

                self.Parameters.completion_status = "Workflow has been failed. See {}".format(exec_workflow_url)
                self.on_workflow_failed(execution_result, self.Parameters.completion_status)

            if self.Parameters.publish_nirvana_output:
                nirvana_results = {}
                result_params = dict(
                    client.make_request(
                        "getWorkflowResults",
                        dict(workflowId=executed_workflow_id, workflowInstanceId=executed_workflow_instance_id),
                    )
                )["results"]
                for result_param in result_params:
                    resource_url = result_param["directStoragePath"]
                    resource_name = result_param["endpoint"]
                    resource_data = client.download_resource(resource_url)
                    nirvana_results.update({resource_name: resource_data})

                self.Parameters.nirvana_results = nirvana_results

                if self.Parameters.process_result:
                    self.process_result(nirvana_results)

                self.Parameters.completion_status = "success"

    def on_exception(self):
        self.Parameters.completion_status = "exception"

    def on_workflow_timeout(self):
        workflow_url = self.Parameters.executed_workflow_url
        logger.info(
            "Task timed out, workflow URL: %s, executed_workflow_id: %s",
            workflow_url,
            self.Parameters.executed_workflow_id,
        )

        if not self.Parameters.executed_workflow_id:
            raise Exception(
                "Task timed out, but Parameters.executed_workflow_id is empty. Could not get running blocks information"
            )
        else:
            self.set_info("Workflow {} had timed out.".format(workflow_url))
        self.cancel_workflow_instance()

    def on_finish(self, prev_status, status):
        self.update_metadata()

    def on_timeout(self, prev_status):
        self.update_metadata()
        self.on_workflow_timeout()

    def on_break(self, prev_status, status):
        self.update_metadata()
        self.cancel_workflow_instance()

    def cancel_workflow_instance(self):
        if not self.Parameters.stop_flow_on_terminate:
            return
        executed_workflow_instance_id = self.Parameters.executed_workflow_instance_id
        self.get_nirvana_client().make_request("stopWorkflow", dict(workflowInstanceId=executed_workflow_instance_id))
        logger.info("Workflow has been stopped.")

    @property
    def footer(self):
        if self.Parameters.executed_workflow_url:
            href = str(self.Parameters.executed_workflow_url)
            return '<a href="{}">Executed workflow</a>'.format(href)
        else:
            href = SdcRunNirvanaWorkflow.build_workflow_url(
                self.Parameters.nirvana_workflow_id,
                self.Parameters.nirvana_workflow_instance_id,
            )
            return 'Workflow not created. <a href="{}">Template workflow</a>'.format(href)

    def create_badge(self, badge_id: str, module: str, url: str, text: str, status: str) -> dict:
        return {"id": badge_id, "module": module, "url": url, "text": text, "status": status}

    def _get_task_description(self) -> str:
        try:
            data = self.server.task[self.id].read(fields=["description", "info"])
            desc = (data or {}).get("description") or ""
            info = (data or {}).get("info") or ""

            return "\n".join([desc, info]).strip()
        except Exception as exc:
            logger.warning("Failed to read task text via API: %s", exc)
            return ""

    @staticmethod
    def _first_group(pattern: re.Pattern, text: str) -> str:
        m = pattern.search(text or "")
        return m.group(1).strip() if m else ""

    def _extract_ci_meta_from_description(self) -> dict[str, str]:
        raw = self._get_task_description()
        text = self._html_to_text(raw)

        def g(pat: re.Pattern) -> str:
            m = pat.search(text)
            return m.group(1).strip() if m else ""

        return {
            "ci_job": g(_CI_JOB_RE),
            "ci_launch": g(_CI_LAUNCH_RE),
        }

    def build_workflow_instance_comment(self) -> str:
        sandbox_task_url = self.build_sandbox_task_url(self.id)
        meta = self._extract_ci_meta_from_description()

        pairs = [
            ("Running from sandbox task", sandbox_task_url),
            ("CI job", meta.get("ci_job", "")),
            ("CI launch", meta.get("ci_launch", "")),
        ]

        parts = [f"{k}: {v}" for k, v in pairs if (v or "").strip()]
        return "\n".join(parts)

