from six.moves.urllib.parse import urljoin

from sandbox.projects.sdc.common.requests_util import log_helper
from sandbox.projects.sdc.common.requests_util import session

from .task_steps_data import TaskStepsData
from .task_state import TaskState
from .base_client import BaseLiteAgentClient


class LiteAgentClient(BaseLiteAgentClient):
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.session = session.create_session()
        self.authorization_header = {"Authorization": f"OAuth {token}"}

    def cancel_task(self, task_id):
        """
        :type task_id: str
        """
        api_url = urljoin(self.base_url, "tasks/{}".format(task_id))
        response = self.session.delete(api_url, headers=self.authorization_header)
        log_helper.log_response(response)
        response.raise_for_status()
        return True

    def create_task(self, dict_params):
        """
        :type dict_params: dict
        """
        api_url = urljoin(self.base_url, "/tasks")
        response = self.session.post(api_url, json=dict_params, headers=self.authorization_header)
        log_helper.log_response(response)
        response.raise_for_status()
        resp_json = response.json()
        return TaskState.from_dict(resp_json, self.base_url)

    def get_task_state(self, task_id):
        """
        :type task_id: str
        :rtype TaskState
        """
        api_url = urljoin(self.base_url, "tasks/{}/status".format(task_id))
        response = self.session.get(api_url, headers=self.authorization_header)

        log_helper.log_response(response)
        response.raise_for_status()
        dict_data = response.json()

        return TaskState.from_dict(dict_data, self.base_url)

    def get_steps_result(self, task_id):
        """
        :type task_id: str
        :rtype TaskResult
        """
        api_url = urljoin(self.base_url, "tasks/{}/steps".format(task_id))
        response = self.session.get(api_url, headers=self.authorization_header)
        log_helper.log_response(response)
        response.raise_for_status()
        json_data = response.json()  # actual data type is list, not dict
        return TaskStepsData.from_json(json_data)

    def change_agent_availability(self, fqdn, availability):
        """
        :type fdqn: str
        :type availability: str
        """
        api_url = urljoin(self.base_url, "admin/agent/{}/status".format(fqdn))
        response = self.session.post(
            url=api_url,
            json={"availability": availability},
            headers=self.authorization_header,
        )
        log_helper.log_response(response)
        response.raise_for_status()
        return True

    def get_target_daemon_version(self):
        """
        :rtype str
        """
        api_url = urljoin(self.base_url, "admin/target-daemon-version")
        response = self.session.get(api_url, headers=self.authorization_header)
        log_helper.log_response(response)
        response.raise_for_status()
        return response.json()["result"]["version"]

