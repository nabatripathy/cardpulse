from __future__ import annotations

import hashlib
from collections import defaultdict
from pathlib import Path

import numpy as np

from cardpulse_frameworks.common.pan import luhn_valid
from cardpulse_synth.generator import SyntheticBank, generate
from conftest import TINY


def _digest(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file() and p.name != "_manifest.json"
    }


def test_same_seed_gives_identical_files(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    generate(TINY, a)
    generate(TINY, b)
    assert _digest(a) == _digest(b)


def test_card_tokens_are_never_luhn_valid():
    bank = SyntheticBank(TINY).build()
    assert len(set(bank.card_token)) == len(bank.card_token)
    assert not any(luhn_valid(t) for t in bank.card_token)
    assert all(t.startswith("99") and len(t) == 16 for t in bank.card_token)


def test_trailers_match_detail_records(tiny_landing):
    root, manifest = tiny_landing
    rejected = set(manifest.defects["rejected_files"])
    for f in root.rglob("*.dat"):
        lines = f.read_text().splitlines()
        details = [ln for ln in lines if ln.startswith("D|")]
        count = int(lines[-1].split("|")[1])
        if f.name in rejected:
            assert count == len(details) + 1  # injected defect
        else:
            assert count == len(details)


def test_never_activated_accounts_have_no_transactions():
    bank = SyntheticBank(TINY).build()
    never = np.nonzero(bank.never_activates)[0]
    assert len(never) > 0
    assert not np.isin(bank.t_acct, never).any()
    assert (bank.card_act[never] == -1).all()


def test_spend_decliners_mostly_meet_the_t02_rule():
    """T02 fires when 30-day spend < 50% of the 6-month average. TINY has only 30 days of
    history, so compare the last 30 days with each account's planned monthly spend."""
    bank = SyntheticBank(TINY).build()
    purchase = (bank.t_type == "05") & (bank.t_txn_o >= bank.bd_o - 30)
    spend30: dict[int, float] = defaultdict(float)
    for a, amt in zip(bank.t_acct[purchase], bank.t_amount[purchase], strict=True):
        spend30[a] += amt
    decliners = np.nonzero(bank.decliner)[0]
    assert len(decliners) > 0
    hits = sum(1 for a in decliners if spend30[a] < 0.5 * bank.monthly_spend[a] * 100)
    assert hits / len(decliners) > 0.8


def test_account_numbers_are_never_luhn_valid():
    bank = SyntheticBank(TINY).build()
    assert len(set(bank.acct_no)) == len(bank.acct_no)
    assert all(len(a) == 16 and a.startswith("70") and not luhn_valid(a) for a in bank.acct_no)
