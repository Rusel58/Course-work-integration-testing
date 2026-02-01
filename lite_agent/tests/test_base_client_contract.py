from sandbox.projects.sdc.common.lite_agent_api.base_client import BaseLiteAgentClient
from sandbox.projects.sdc.common.lite_agent_api.client import LiteAgentClient
from sandbox.projects.sdc.common.lite_agent_api.dry_run_client import LiteAgentDryRunClient


def test_classes_implement_base_interface():
    assert issubclass(LiteAgentClient, BaseLiteAgentClient)
    assert issubclass(LiteAgentDryRunClient, BaseLiteAgentClient)

