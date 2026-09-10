"""The operator guide may offer existing workflows, never evidence verdicts."""

from pathlib import Path

from qa_lab.qa_workflows import WORKFLOWS, workflow_guide_html


def test_missing_workflow_sources_do_not_offer_launch_requests(tmp_path):
    page = workflow_guide_html(tmp_path)
    for workflow in WORKFLOWS:
        assert workflow.title in page
    assert "<pre>" not in page
    assert page.count("No request offered") == len(WORKFLOWS)


def test_complete_guide_uses_existing_fork_workflows_and_discloses_claim_limits():
    root = Path(__file__).resolve().parents[3]
    for workflow in WORKFLOWS:
        assert all((root / source).is_file() for source in workflow.sources)
    page = workflow_guide_html(root)
    assert page.count("<pre>") == len(WORKFLOWS)
    assert "intent.uniswap_v4.robinhood.SWAP.anvil.safe" in page
    assert "intent.uniswap_v4.robinhood.SWAP.anvil.eoa" in page
    assert "do not prove live equity-reference freshness" in page
    assert "never dispatches work or changes coverage" in page
    assert "none sends mainnet transactions" in page
