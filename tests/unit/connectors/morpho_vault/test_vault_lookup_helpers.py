"""Hermetic contract tests for the Morpho vault lookup's payload helpers.

``_has_items_list`` decides whether a 200 GraphQL body is a real answer (a
list at ``data.<root>.items``, empty or not) or a malformed one that must be
treated as a FAILED generation so a partial index is never cached.
"""

from __future__ import annotations

import pytest

from almanak.connectors.morpho_vault.gateway.vault_lookup import _has_items_list, tag_vault_payload


@pytest.mark.parametrize(
    "body",
    [
        {"data": {"vaults": {"items": []}}},
        {"data": {"vaults": {"items": [{"address": "0xabc"}]}}},
        {"data": {"vaults": {"items": []}, "unrelated": 1}},
    ],
)
def test_real_list_at_root_items_is_an_answer(body):
    assert _has_items_list(body, "vaults") is True


@pytest.mark.parametrize(
    "body",
    [
        None,
        "not a dict",
        [],
        {},
        {"data": None},
        {"data": {}},
        {"data": {"vaults": None}},
        {"data": {"vaults": {}}},
        {"data": {"vaults": {"items": None}}},
        {"data": {"vaults": {"items": {"address": "0xabc"}}}},
        {"data": {"vaultV2s": {"items": []}}},  # right shape, wrong root
    ],
)
def test_missing_or_malformed_items_is_not_an_answer(body):
    assert _has_items_list(body, "vaults") is False


def test_tag_vault_payload_tags_and_skips_non_dict_entries():
    body = {"data": {"vaultV2s": {"items": [{"address": "0xabc", "symbol": "steakUSDC"}, "junk", None]}}}
    assert tag_vault_payload(body, "vaultV2s", "v2") == [
        {"address": "0xabc", "symbol": "steakUSDC", "vault_version": "v2"}
    ]


def test_tag_vault_payload_empty_list_and_logs_on_graphql_error(caplog):
    body = {
        "errors": [{"message": 'Field "whitelisted" is not defined by type "VaultFilters"'}],
        "data": None,
    }
    with caplog.at_level("WARNING"):
        assert tag_vault_payload(body, "vaults", "v1") == []
    assert "whitelisted" in caplog.text
    assert "vaults" in caplog.text


def test_truncated_page_is_not_an_answer():
    from almanak.connectors.morpho_vault.gateway.vault_lookup import _is_truncated_page

    body = {"data": {"vaults": {"items": [{"address": "0x1"}], "pageInfo": {"count": 1, "countTotal": 2}}}}
    assert _is_truncated_page(body, "vaults") is True
    complete = {"data": {"vaults": {"items": [{"address": "0x1"}], "pageInfo": {"count": 1, "countTotal": 1}}}}
    assert _is_truncated_page(complete, "vaults") is False
    assert _is_truncated_page({"data": {"vaults": {"items": []}}}, "vaults") is False  # no pageInfo: cannot judge
