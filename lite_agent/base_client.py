from abc import ABC, abstractmethod
from typing import Dict

from .task_state import TaskState
from .task_steps_data import TaskStepsData


class BaseLiteAgentClient(ABC):
    """
    The basic interface of the LiteAgent API client.
    Both clients (real and dry-run) are required to implement the same methods.
    """

    @abstractmethod
    def create_task(self, dict_params: Dict) -> TaskState:
        """Create a task and return its status."""

    @abstractmethod
    def get_task_state(self, task_id: str) -> TaskState:
        """Get the current issue status."""

    @abstractmethod
    def cancel_task(self, task_id: str) -> bool:
        """Cancel the task, return True on success."""

    @abstractmethod
    def get_steps_result(self, task_id: str) -> TaskStepsData:
        """Get the steps/resources of the task (for reports/links)."""

    @abstractmethod
    def change_agent_availability(self, fqdn: str, availability: str) -> bool:
        """Change the agent's availability (for a real API, there may be a no-op in dry-run)."""

    @abstractmethod
    def get_target_daemon_version(self) -> str:
        """Return the target daemon version (or 'dry-run' for the emulator)."""

