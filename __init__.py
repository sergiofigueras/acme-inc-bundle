"""ACME Inc tenant plugin."""

from __future__ import annotations

from .toolkit import AcmeIncToolkit
from .workflow import TelefonicaWorkflow
from server.orchestrator_graph import get_orchestrator_graph

PARTNER_ID = "acme-inc"
ALIASES = ["acme-inc"]


def create_toolkit() -> AcmeIncToolkit:
    return AcmeIncToolkit()


def get_workflow() -> AcmeIncWorkflow:
    return AcmeIncWorkflow()


def get_graph():
    """Return ACME Inc's LangGraph.

    For now we reuse the system default graph topology, and ACME Inc's
    behavior is driven by its workflow + toolkit.

    This is the extension point to introduce a ACME Inc-specific graph.
    """

    return get_orchestrator_graph()
