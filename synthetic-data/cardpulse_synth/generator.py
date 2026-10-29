"""Generate a fictitious card portfolio and write it as landing files.

The portfolio is built to exercise the MVP:

* T01 population: new accounts whose card was issued but never used.
* T02 population: seasoned accounts whose last-30-day spend falls well below
  their 6-month average.
* Suppression populations: closed, charged-off, blocked, delinquent, fraud-flagged,
  deceased, opted-out and do-not-solicit customers, plus new accounts already
  earning a sign-up bonus (S12).

Everything is seeded, so the same config always produces the same files.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import numpy as np

from cardpulse_frameworks.common.pan import luhn_check_digit

from . import reference as ref
from .config import SynthConfig
from .writers import write_csv, write_hdt

EPOCH = date(1970, 1, 1)
NEVER = np.iinfo(np.int64).max // 2

POSTED_TXN_COLUMNS = [
    "TXN_REF_NO",
    "PAN_TOKEN",
    "ACCT_NO",
    "POST_DT",
    "TXN_DT",
    "TXN_TM",
    "TXN_AMT",
    "CURR_CD",
    "MERCH_ID",
    "MERCH_NM",
    "MCC",
    "TXN_TYPE",
    "ENTRY_MODE",
    "ORIG_TXN_REF_NO",
    "TZ_CD",
]
CUSTOMER_COLUMNS = [
    "CUST_ID",
    "FIRST_NM",
    "LAST_NM",
    "BIRTH_YR",
    "STATE_CD",
    "ZIP3",
    "CUST_SINCE_DT",
    "SEGMENT_CD",
    "DECEASED_FLG",
]
ACCOUNT_COLUMNS = [
    "ACCT_NO",
    "PRODUCT_CD",
    "OPEN_DT",
    "CLOSE_DT",
    "STATUS_CD",
    "CREDIT_LIMIT",
    "APR_BPS",
    "CYCLE_DAY",
    "FRAUD_FLG",
    "ACQ_BONUS_FLG",
]
ROLE_COLUMNS = ["CUST_ID", "ACCT_NO", "ROLE_CD", "EFF_DT"]
CARD_COLUMNS = ["PAN_TOKEN", "ACCT_NO", "CUST_ID", "ISSUE_DT", "ACTIVATION_DT", "EXPIRY_YM", "WALLET_FLG"]
BALANCE_COLUMNS = ["ACCT_NO", "BUS_DT", "STMT_BAL", "CURR_BAL", "REVOLV_BAL", "AVAIL_CREDIT", "DAYS_PAST_DUE"]
MERCHANT_COLUMNS = ["MERCH_ID", "MERCH_NM", "MCC", "CITY", "STATE_CD", "COUNTRY_CD"]
MCC_COLUMNS = ["MCC", "MCC_DESC", "CATEGORY_GRP"]
CONSENT_COLUMNS = ["customer_id", "channel_cd", "purpose_cd", "consent_status", "source_cd", "captured_ts"]
CAMPAIGN_COLUMNS = [
    "campaign_id",
    "campaign_name",
    "trigger_cd",
    "offer_cd",
    "offer_value",
    "est_cost_usd",
    "start_date",
    "end_date",
    "compliance_approval_id",
    "product_scope",
]


def ymd(days: np.ndarray) -> np.ndarray:
    """Days since epoch -> 'YYYYMMDD' strings."""
    s = np.datetime_as_string(np.asarray(days, dtype="int64").astype("datetime64[D]"), unit="D")
    return np.char.replace(s, "-", "")


def ymd_or_blank(days: np.ndarray, missing: np.ndarray) -> np.ndarray:
    return np.where(missing, "", ymd(np.where(missing, 0, days)))


def to_ord(d: date) -> int:
    return (d - EPOCH).days


def make_tokens(rng: np.random.Generator, n: int) -> np.ndarray:
    """16-digit format-preserving card tokens in a reserved '99' range, Luhn-invalid by design."""
    bodies: set[int] = set()
    while len(bodies) < n:
        bodies.update(rng.integers(0, 10**13, n - len(bodies)).tolist())
    out = []
    for b in sorted(bodies):
        partial = f"99{b:013d}"
        out.append(partial + str((luhn_check_digit(partial) + 1) % 10))
    arr = np.array(out)
    rng.shuffle(arr)
    return arr


@dataclass
class Manifest:
    business_date: str
    seed: int
    files: dict[str, int] = field(default_factory=dict)
    rows: dict[str, int] = field(default_factory=dict)
    ground_truth: dict[str, int] = field(default_factory=dict)
    defects: dict[str, object] = field(default_factory=dict)
    prior_snapshot: dict[str, object] = field(default_factory=dict)

    def write(self, root: Path) -> Path:
        path = root / "_manifest.json"
        path.write_text(json.dumps(self.__dict__, indent=2))
        return path


class SyntheticBank:
    def __init__(self, cfg: SynthConfig):
        self.cfg = cfg
        self.rng = np.random.default_rng(cfg.seed)
        self.bd = cfg.business_date
        self.bd_o = to_ord(cfg.business_date)

    # ------------------------------------------------------------------ build
    def build(self) -> SyntheticBank:
        self._customers()
        self._accounts()
        self._roles_and_cards()
        self._merchants()
        self._transactions()
        self._balances()
        return self

    def _customers(self) -> None:
        cfg, rng = self.cfg, self.rng
        n = cfg.n_customers
        self.cust_id = np.array([f"C{100_000_001 + i}" for i in range(n)])
        w = np.array([s[1] for s in ref.STATES])
        self.state_p = w / w.sum()
        self.cust_state = rng.choice(len(ref.STATES), n, p=self.state_p)
        zip_table = np.array([s[3] for s in ref.STATES])
        self.cust_zip3 = zip_table[self.cust_state, rng.integers(0, 3, n)]
        self.cust_first = rng.choice(ref.FIRST_NAMES, n)
        self.cust_last = rng.choice(ref.LAST_NAMES, n)
        self.cust_birth = rng.integers(1945, 2006, n)
        self.cust_segment = rng.choice(["MASS", "AFFLUENT", "PREMIER", "STUDENT"], n, p=[0.62, 0.25, 0.08, 0.05])
        self.cust_deceased = rng.random(n) < cfg.deceased_rate

    def _accounts(self) -> None:
        cfg, rng, bd = self.cfg, self.rng, self.bd_o
        n = cfg.n_customers
        extra = round(n * cfg.second_account_rate)
        self.acct_cust = np.concatenate([np.arange(n), rng.integers(0, n, extra)])
        na = self.na = len(self.acct_cust)
        # 16-digit account numbers in a non-card '70' range, Luhn-invalid so no scanner mistakes them for PANs.
        bodies = [f"70{1_000_000_000_000 + i:013d}" for i in rng.permutation(na)]
        self.acct_no = np.array([b + str((luhn_check_digit(b) + 1) % 10) for b in bodies])

        names = list(ref.PRODUCTS)
        p = np.array([ref.PRODUCTS[k]["weight"] for k in names])
        self.acct_product = rng.choice(names, na, p=p / p.sum())

        self.is_new = rng.random(na) < cfg.new_account_rate
        self.open_o = np.where(self.is_new, bd - rng.integers(0, 61, na), bd - rng.integers(61, 15 * 365, na)).astype(
            np.int64
        )

        seasoned = ~self.is_new
        r = rng.random(na)
        c1, c2 = cfg.closed_rate, cfg.closed_rate + cfg.charged_off_rate
        c3 = c2 + cfg.blocked_rate
        closed = seasoned & (r < c1)
        charged = seasoned & (r >= c1) & (r < c2)
        blocked = seasoned & (r >= c2) & (r < c3)
        self.acct_status = np.full(na, "OPEN", dtype="<U11")
        self.acct_status[closed] = "CLOSED"
        self.acct_status[charged] = "CHARGED_OFF"
        self.acct_status[blocked] = "BLOCKED"

        end = np.full(na, NEVER, dtype=np.int64)  # first day with no new spend
        end[closed] = bd - rng.integers(1, 300, closed.sum())
        end[charged] = bd - rng.integers(30, 300, charged.sum())
        end[blocked] = bd - rng.integers(1, 60, blocked.sum())
        stopped = closed | charged | blocked
        end[stopped] = np.maximum(end[stopped], self.open_o[stopped] + 30)
        self.end_o = end
        self.close_o = np.where(closed | charged, end, -1)

        median = np.array([ref.PRODUCTS[pr]["limit"] for pr in self.acct_product])
        self.credit_limit = np.clip(np.round(median * rng.lognormal(0, 0.5, na) / 100) * 100, 500, 100_000)
        base = np.array([ref.PRODUCTS[pr]["apr_bps"] for pr in self.acct_product])
        self.apr_bps = (base + rng.integers(-12, 13, na) * 25).astype(int)
        self.cycle_day = rng.integers(1, 29, na)
        self.fraud = rng.random(na) < cfg.fraud_flag_rate
        self.acq_bonus = (
            self.is_new & np.isin(self.acct_product, ref.BONUS_PRODUCTS) & (rng.random(na) < cfg.acquisition_bonus_rate)
        )

        cust_since = np.full(n, bd, dtype=np.int64)
        np.minimum.at(cust_since, self.acct_cust, self.open_o)
        self.cust_since = cust_since

    def _roles_and_cards(self) -> None:
        cfg, rng, bd, na, n = self.cfg, self.rng, self.bd_o, self.na, self.cfg.n_customers
        au_acct = np.nonzero(rng.random(na) < cfg.authorized_user_rate)[0]
        au_cust = rng.integers(0, n, len(au_acct))
        same = au_cust == self.acct_cust[au_acct]
        au_cust[same] = (au_cust[same] + 1) % n
        au_eff = np.minimum(self.open_o[au_acct] + rng.integers(0, 365, len(au_acct)), bd)
        self.au_acct, self.au_cust, self.au_eff = au_acct, au_cust, au_eff

        prim_issue = np.where(
            self.is_new,
            self.open_o + rng.integers(2, 8, na),
            np.maximum(self.open_o + 3, bd - rng.integers(0, 1095, na)),
        )
        prim_issue = np.minimum(prim_issue, bd)
        au_issue = np.minimum(au_eff + rng.integers(2, 8, len(au_acct)), bd)

        self.card_acct = np.concatenate([np.arange(na), au_acct])
        self.card_cust = np.concatenate([self.acct_cust, au_cust])
        self.card_issue = np.concatenate([prim_issue, au_issue]).astype(np.int64)
        nc = len(self.card_acct)

        self.never_activates = self.is_new & (rng.random(na) < cfg.non_activation_rate)
        act = self.card_issue + rng.integers(1, 10, nc)
        act[:na][self.never_activates] = -1
        act[act > bd] = -1
        self.card_act = act
        self.card_token = make_tokens(rng, nc)
        self.card_wallet = (act >= 0) & (rng.random(nc) < 0.35)

        self.au_card_for_acct = np.full(na, -1)
        self.au_card_for_acct[au_acct] = na + np.arange(len(au_acct))

    def _merchants(self) -> None:
        rng = self.rng
        mcc_w = np.array([m[4] for m in ref.MCC_TABLE])
        nm = int(np.clip(self.cfg.n_customers // 5, 500, 50_000))
        mcc_idx = rng.choice(len(ref.MCC_TABLE), nm, p=mcc_w / mcc_w.sum())
        atm_idx = next(i for i, m in enumerate(ref.MCC_TABLE) if m[2] == "CASH")
        n_atm = 20
        self.merch_mcc_idx = np.concatenate([mcc_idx, np.full(n_atm, atm_idx)])
        total = nm + n_atm
        self.merch_id = np.array([f"M{i:08d}" for i in range(1, total + 1)])
        self.merch_state = rng.choice(len(ref.STATES), total, p=self.state_p)
        city_table = np.array([s[4] for s in ref.STATES])
        self.merch_city = city_table[self.merch_state, rng.integers(0, 3, total)]
        words = rng.choice(ref.MERCHANT_WORDS, total)
        nouns = [ref.CATEGORY_NOUN[ref.MCC_TABLE[i][2]] for i in self.merch_mcc_idx]
        nums = rng.integers(100, 9999, total)
        self.merch_name = np.array([f"{w} {nn} #{k}" for w, nn, k in zip(words, nouns, nums, strict=True)])
        pop = rng.lognormal(0, 1, total)
        pop[nm:] = 0
        self.merch_pop = pop / pop.sum()
        self.atm_range = (nm, total)
        tz_table = np.array([s[2] for s in ref.STATES])
        self.merch_tz = tz_table[self.merch_state]

    def _transactions(self) -> None:
        cfg, rng, bd, na = self.cfg, self.rng, self.bd_o, self.na
        monthly = np.clip(rng.lognormal(np.log(cfg.median_monthly_spend), 0.8, na), 20, 25_000)
        monthly[rng.random(na) < cfg.dormant_rate] = 0
        self.monthly_spend = monthly
        daily = monthly / cfg.avg_ticket / 30.4
        seasoned6 = (~self.is_new) & (self.open_o <= bd - 183)
        self.decliner = (
            seasoned6 & (monthly >= 300) & (self.acct_status == "OPEN") & (rng.random(na) < cfg.spend_decline_rate)
        )
        factor = np.where(self.decliner, rng.uniform(0.10, 0.40, na), 1.0)
        start = np.where(self.card_act[:na] >= 0, self.card_act[:na], NEVER)
        weekday_factor = np.array([0.95, 0.95, 1.0, 1.0, 1.15, 1.2, 0.85])  # Monday..Sunday

        accts, days, kinds = [], [], []  # kind 0 = card transaction, 1 = payment
        first = bd - cfg.history_days
        cal = np.arange(first, bd + 1).astype("datetime64[D]")
        dom = (cal - cal.astype("datetime64[M]")).astype(int) + 1  # day of month
        for k, d in enumerate(range(first, bd + 1)):
            active = (start <= d) & (d < self.end_o) & (daily > 0)
            idx = np.nonzero(active)[0]
            lam = daily[idx] * weekday_factor[(d + 3) % 7]
            if d >= bd - 30:
                lam = lam * factor[idx]
            counts = rng.poisson(lam)
            a = np.repeat(idx, counts)
            accts.append(a)
            days.append(np.full(len(a), d))
            kinds.append(np.zeros(len(a), dtype=np.int8))
            payers = idx[self.cycle_day[idx] == dom[k]]
            accts.append(payers)
            days.append(np.full(len(payers), d))
            kinds.append(np.ones(len(payers), dtype=np.int8))

        acct = np.concatenate(accts)
        txn_o = np.concatenate(days).astype(np.int64)
        kind = np.concatenate(kinds)
        nt = len(acct)

        u = rng.random(nt)
        ttype = np.where(kind == 1, "21", np.where(u < 0.015, "06", np.where(u < 0.02, "07", "05")))
        merch = rng.choice(len(self.merch_id), nt, p=self.merch_pop)
        cash = ttype == "07"
        merch[cash] = rng.integers(self.atm_range[0], self.atm_range[1], cash.sum())
        merch[kind == 1] = -1

        ticket = np.array([m[3] for m in ref.MCC_TABLE])
        mcc_idx = self.merch_mcc_idx[np.maximum(merch, 0)]
        amount = np.maximum(np.round(rng.lognormal(np.log(ticket[mcc_idx]), 0.6) * 100), 100).astype(np.int64)
        amount[cash] = rng.choice([40, 60, 100, 200, 300, 500], cash.sum()) * 100
        refund = ttype == "06"
        amount[refund] = -np.maximum(np.round(amount[refund] * 0.6), 100)
        pay = kind == 1
        amount[pay] = -np.maximum(np.round(monthly[acct[pay]] * rng.uniform(0.2, 1.1, pay.sum()) * 100), 2500)

        hours_p = np.array([1, 1, 1, 1, 1, 2, 3, 5, 6, 6, 7, 8, 9, 8, 7, 7, 7, 8, 8, 7, 6, 4, 3, 2], dtype=float)
        secs = rng.choice(24, nt, p=hours_p / hours_p.sum()) * 3600 + rng.integers(0, 3600, nt)
        lag = np.where(pay, 0, rng.choice([0, 1, 2], nt, p=[0.55, 0.38, 0.07]))
        post_o = txn_o + lag
        post_secs = np.where(
            lag == 0, np.minimum(secs + rng.integers(60, 5400, nt), 86_399), rng.integers(0, 86_400, nt)
        )

        # Card used: primary, or the authorized user's card once it is active.
        au = self.au_card_for_acct[acct]
        use_au = (au >= 0) & (rng.random(nt) < 0.25) & ~pay
        use_au &= (self.card_act[np.maximum(au, 0)] >= 0) & (self.card_act[np.maximum(au, 0)] <= txn_o)
        card = np.where(use_au, au, acct)

        keep = post_o <= bd
        order = np.lexsort((post_secs[keep], post_o[keep]))
        sel = np.nonzero(keep)[0][order]
        self.t_acct, self.t_card, self.t_txn_o, self.t_post_o = acct[sel], card[sel], txn_o[sel], post_o[sel]
        self.t_secs, self.t_post_secs, self.t_amount = secs[sel], post_secs[sel], amount[sel]
        self.t_type, self.t_merch, self.t_kind = ttype[sel], merch[sel], kind[sel]
        self.t_entry = np.where(
            self.t_kind == 1,
            "00",
            np.where(self.t_type == "07", "05", rng.choice(ref.ENTRY_MODES[0], len(sel), p=ref.ENTRY_MODES[1])),
        )
        # Reference number: post date + running sequence within the post date.
        _, first_idx, counts = np.unique(self.t_post_o, return_index=True, return_counts=True)
        seq = np.arange(len(sel)) - np.repeat(first_idx, counts)
        self.t_ref = np.char.add(ymd(self.t_post_o), np.char.zfill((seq + 1).astype(str), 10))
        self.t_batch = np.where(self.t_post_o == bd, self.t_post_secs * cfg.intraday_batches // 86_400 + 1, 1).astype(
            int
        )

    def _balances(self) -> None:
        cfg, rng, bd, na = self.cfg, self.rng, self.bd_o, self.na
        window = (self.t_txn_o >= bd - 30) & (self.t_txn_o < bd) & np.isin(self.t_type, ["05", "07"])
        spend30 = np.bincount(self.t_acct[window], weights=self.t_amount[window], minlength=na)
        limit_c = self.credit_limit * 100
        self.revolver = rng.random(na) < cfg.revolver_rate
        curr = spend30 + np.where(self.revolver, rng.uniform(0.1, 0.7, na) * limit_c, 0)
        curr = np.minimum(curr, limit_c * 1.05)
        curr[self.acct_status == "CLOSED"] = 0
        stmt = curr * rng.uniform(0.6, 1.0, na)
        revolv = np.where(self.revolver, stmt * rng.uniform(0.5, 1.0, na), 0)
        avail = np.where(np.isin(self.acct_status, ["OPEN"]), np.maximum(limit_c - curr, 0), 0)
        dpd = np.zeros(na, dtype=int)
        delinquent = (self.acct_status == "OPEN") & ~self.is_new & (rng.random(na) < cfg.delinquent_rate)
        dpd[delinquent] = rng.choice([15, 30, 45, 60, 90], delinquent.sum(), p=[0.4, 0.3, 0.12, 0.1, 0.08])
        dpd[self.acct_status == "CHARGED_OFF"] = 180
        self.delinquent = delinquent
        self.bal = {
            "STMT_BAL": np.round(stmt).astype(np.int64),
            "CURR_BAL": np.round(curr).astype(np.int64),
            "REVOLV_BAL": np.round(revolv).astype(np.int64),
            "AVAIL_CREDIT": np.round(avail).astype(np.int64),
            "DAYS_PAST_DUE": dpd,
        }

    # ------------------------------------------------------------------ write
    def write(self, root: Path) -> Manifest:
        root.mkdir(parents=True, exist_ok=True)
        bd = self.bd
        m = Manifest(business_date=bd.isoformat(), seed=self.cfg.seed)
        self._snap: dict[str, list] = {}

        def track(entity: str, files: int, rows: int) -> None:
            m.files[entity] = m.files.get(entity, 0) + files
            m.rows[entity] = m.rows.get(entity, 0) + rows

        # Customers
        cust_rows = list(
            zip(
                self.cust_id.tolist(),
                self.cust_first.tolist(),
                self.cust_last.tolist(),
                self.cust_birth.astype(str).tolist(),
                [ref.STATES[s][0] for s in self.cust_state],
                self.cust_zip3.tolist(),
                ymd(self.cust_since).tolist(),
                self.cust_segment.tolist(),
                np.where(self.cust_deceased, "Y", "N").tolist(),
                strict=True,
            )
        )
        write_hdt(root, "processor", "customer", "CUSTOMER", bd, 1, CUSTOMER_COLUMNS, cust_rows)
        track("processor.customer", 1, len(cust_rows))
        self._snap["customer"] = cust_rows

        # Accounts
        acct_rows = list(
            zip(
                self.acct_no.tolist(),
                self.acct_product.tolist(),
                ymd(self.open_o).tolist(),
                ymd_or_blank(self.close_o, self.close_o < 0).tolist(),
                self.acct_status.tolist(),
                self.credit_limit.astype(int).astype(str).tolist(),
                self.apr_bps.astype(str).tolist(),
                self.cycle_day.astype(str).tolist(),
                np.where(self.fraud, "Y", "N").tolist(),
                np.where(self.acq_bonus, "Y", "N").tolist(),
                strict=True,
            )
        )
        write_hdt(root, "processor", "account", "ACCOUNT", bd, 1, ACCOUNT_COLUMNS, acct_rows)
        track("processor.account", 1, len(acct_rows))
        self._snap["account"] = acct_rows

        # Customer-account roles
        role_cust = np.concatenate([self.acct_cust, self.au_cust])
        role_acct = np.concatenate([np.arange(self.na), self.au_acct])
        role_cd = np.concatenate([np.full(self.na, "P"), np.full(len(self.au_acct), "A")])
        role_eff = np.concatenate([self.open_o, self.au_eff])
        role_rows = list(
            zip(
                self.cust_id[role_cust].tolist(),
                self.acct_no[role_acct].tolist(),
                role_cd.tolist(),
                ymd(role_eff).tolist(),
                strict=True,
            )
        )
        write_hdt(root, "processor", "cust_acct_role", "CUST_ACCT_ROLE", bd, 1, ROLE_COLUMNS, role_rows)
        track("processor.cust_acct_role", 1, len(role_rows))
        self._snap["cust_acct_role"] = role_rows

        # Cards
        expiry = np.char.replace(
            np.datetime_as_string(
                self.card_issue.astype("datetime64[D]").astype("datetime64[M]") + np.timedelta64(48, "M"), unit="M"
            ),
            "-",
            "",
        )
        card_rows = list(
            zip(
                self.card_token.tolist(),
                self.acct_no[self.card_acct].tolist(),
                self.cust_id[self.card_cust].tolist(),
                ymd(self.card_issue).tolist(),
                ymd_or_blank(self.card_act, self.card_act < 0).tolist(),
                expiry.tolist(),
                np.where(self.card_wallet, "Y", "N").tolist(),
                strict=True,
            )
        )
        write_hdt(root, "processor", "card", "CARD", bd, 1, CARD_COLUMNS, card_rows)
        track("processor.card", 1, len(card_rows))
        self._snap["card"] = card_rows

        # Merchants and MCC reference
        merch_rows = list(
            zip(
                self.merch_id.tolist(),
                self.merch_name.tolist(),
                [ref.MCC_TABLE[i][0] for i in self.merch_mcc_idx],
                self.merch_city.tolist(),
                [ref.STATES[s][0] for s in self.merch_state],
                ["US"] * len(self.merch_id),
                strict=True,
            )
        )
        write_hdt(root, "processor", "merchant", "MERCHANT", bd, 1, MERCHANT_COLUMNS, merch_rows)
        track("processor.merchant", 1, len(merch_rows))
        self._snap["merchant"] = merch_rows
        mcc_rows = [(m_[0], m_[1], m_[2]) for m_ in ref.MCC_TABLE]
        write_csv(root, "reference", "mcc", bd, 1, MCC_COLUMNS, mcc_rows)
        track("reference.mcc", 1, len(mcc_rows))
        self._snap["mcc"] = mcc_rows

        # Daily balances (prior-day close, delivered on the business date)
        bal_rows = list(
            zip(
                self.acct_no.tolist(),
                [f"{bd:%Y%m%d}"] * self.na,
                *(self.bal[c].astype(str).tolist() for c in BALANCE_COLUMNS[2:]),
                strict=True,
            )
        )
        write_hdt(
            root, "processor", "acct_balance", "ACCT_BALANCE", bd, 1, BALANCE_COLUMNS, bal_rows, amount_field="CURR_BAL"
        )
        track("processor.acct_balance", 1, len(bal_rows))

        self._write_consent(root, track)
        self._write_campaigns(root, track)
        self._write_transactions(root, m, track)

        if self.cfg.prior_snapshot:
            self._write_prior_snapshot(root, m, track)

        m.ground_truth = {
            "accounts": int(self.na),
            "customers": int(self.cfg.n_customers),
            "cards": int(len(self.card_token)),
            "new_accounts_never_activated": int(self.never_activates.sum()),
            "t01_candidates_issued_10_to_60_days": int(
                (
                    self.never_activates
                    & (self.card_issue[: self.na] <= self.bd_o - 10)
                    & (self.card_issue[: self.na] >= self.bd_o - 60)
                ).sum()
            ),
            "designed_spend_decliners": int(self.decliner.sum()),
            "delinquent_accounts": int(self.delinquent.sum()),
            "acquisition_bonus_accounts": int(self.acq_bonus.sum()),
        }
        m.write(root)
        return m

    def _write_prior_snapshot(self, root: Path, m: Manifest, track) -> None:
        """Master files for the day before the business date, so Silver builds real SCD2 history.

        Differences from today's snapshot, all from a separate random stream so the
        main portfolio (and every transaction file) is unchanged:
        accounts, customers, roles and cards created today are absent; some accounts had
        a lower credit limit (credit line increase today); some customers were in another
        segment; cards activated today were not yet active; some merchants were renamed;
        some marketing preferences were the opposite (changed today).
        """
        cfg, prior = self.cfg, self.bd - timedelta(days=1)
        rng2 = np.random.default_rng(cfg.seed + 104_729)
        today = f"{self.bd:%Y%m%d}"
        changes: dict[str, int] = {}

        new_accts = {self.acct_no[i] for i in np.nonzero(self.open_o == self.bd_o)[0]}
        new_custs = {self.cust_id[i] for i in np.nonzero(self.cust_since == self.bd_o)[0]}

        cust = []
        for r in self._snap["customer"]:
            if r[0] in new_custs:
                continue
            if rng2.random() < cfg.prior_segment_change_rate:
                r = (*r[:7], "MASS" if r[7] != "MASS" else "AFFLUENT", r[8])
                changes["customer_segment"] = changes.get("customer_segment", 0) + 1
            cust.append(r)
        acct = []
        for r in self._snap["account"]:
            if r[0] in new_accts:
                continue
            if r[4] == "OPEN" and int(r[5]) > 1000 and rng2.random() < cfg.prior_cli_rate:
                r = (*r[:5], str(max(500, int(r[5]) - int(rng2.choice([500, 1000, 1500, 2000])))), *r[6:])
                changes["account_credit_limit"] = changes.get("account_credit_limit", 0) + 1
            acct.append(r)
        roles = [r for r in self._snap["cust_acct_role"] if r[1] not in new_accts and r[3] != today]
        cards = []
        for r in self._snap["card"]:
            if r[1] in new_accts or r[3] == today:
                continue
            if r[4] == today:
                r = (*r[:4], "", *r[5:])
                changes["card_activated_today"] = changes.get("card_activated_today", 0) + 1
            cards.append(r)
        merch = []
        for r in self._snap["merchant"]:
            if rng2.random() < cfg.prior_merchant_rename_rate:
                r = (r[0], r[1].rsplit(" #", 1)[0] + " INC", *r[2:])
                changes["merchant_renamed"] = changes.get("merchant_renamed", 0) + 1
            merch.append(r)
        consent = [r for r in self._snap.get("consent_prior", []) if r[0] not in new_custs]
        changes["consent_changed_today"] = sum(
            1 for a, b in zip(self._snap.get("consent_prior", []), self._consent_today, strict=True) if a[3] != b[3]
        )

        for entity, ftype, cols, rows in [
            ("customer", "CUSTOMER", CUSTOMER_COLUMNS, cust),
            ("account", "ACCOUNT", ACCOUNT_COLUMNS, acct),
            ("cust_acct_role", "CUST_ACCT_ROLE", ROLE_COLUMNS, roles),
            ("card", "CARD", CARD_COLUMNS, cards),
            ("merchant", "MERCHANT", MERCHANT_COLUMNS, merch),
        ]:
            write_hdt(root, "processor", entity, ftype, prior, 1, cols, rows)
            track(f"processor.{entity}", 1, len(rows))
        write_csv(root, "reference", "mcc", prior, 1, MCC_COLUMNS, self._snap["mcc"])
        track("reference.mcc", 1, len(self._snap["mcc"]))
        write_csv(root, "prefcenter", "consent", prior, 1, CONSENT_COLUMNS, consent)
        track("prefcenter.consent", 1, len(consent))
        write_csv(root, "campaign", "campaign", prior, 1, CAMPAIGN_COLUMNS, self._snap["campaign"])
        track("campaign.campaign", 1, len(self._snap["campaign"]))
        changes["accounts_opened_today"] = len(new_accts)
        m.prior_snapshot = {"business_date": prior.isoformat(), "changes": changes}

    def _write_consent(self, root: Path, track) -> None:
        rng, n = self.rng, self.cfg.n_customers
        rows = []
        sources = np.array(["APP", "WEB", "CALL_CENTER", "BRANCH", "MAIL"])
        for channel, rate in ref.CHANNELS.items():
            status = np.where(rng.random(n) < rate, "OPT_IN", "OPT_OUT")
            src = rng.choice(sources, n)
            ts = self.cust_since + (rng.random(n) * (self.bd_o - self.cust_since)).astype(np.int64)
            secs = rng.integers(0, 86_400, n)
            iso = np.char.add(
                np.datetime_as_string(ts.astype("datetime64[D]"), unit="D"),
                np.char.add("T", [f"{s // 3600:02d}:{s % 3600 // 60:02d}:{s % 60:02d}Z" for s in secs]),
            )
            rows += list(
                zip(
                    self.cust_id.tolist(),
                    [channel] * n,
                    ["MARKETING"] * n,
                    status.tolist(),
                    src.tolist(),
                    iso.tolist(),
                    strict=True,
                )
            )
        dns = np.nonzero(rng.random(n) < self.cfg.do_not_solicit_rate)[0]
        rows += [
            (
                self.cust_id[i],
                "ALL",
                "DO_NOT_SOLICIT",
                "ACTIVE",
                "CALL_CENTER",
                f"{self.bd - timedelta(days=int(rng.integers(1, 400)))}T09:00:00Z",
            )
            for i in dns
        ]
        if self.cfg.prior_snapshot:
            # Preferences changed today: today's row shows the new status captured this morning;
            # the prior-day snapshot keeps the old status.
            rng2 = np.random.default_rng(self.cfg.seed + 7919)
            flip = np.nonzero(rng2.random(len(rows)) < self.cfg.prior_consent_change_rate)[0]
            prior = list(rows)
            for i in flip:
                c, ch, pur, st, src, _ = rows[i]
                if pur != "MARKETING":
                    continue
                prior[i] = (c, ch, pur, "OPT_OUT" if st == "OPT_IN" else "OPT_IN", src, rows[i][5])
                rows[i] = (c, ch, pur, st, "APP", f"{self.bd}T{8 + int(rng2.integers(0, 4)):02d}:15:00Z")
            self._snap["consent_prior"] = prior
            self._consent_today = rows
        write_csv(root, "prefcenter", "consent", self.bd, 1, CONSENT_COLUMNS, rows)
        track("prefcenter.consent", 1, len(rows))

    def _write_campaigns(self, root: Path, track) -> None:
        start = date(self.bd.year, 1, 1).isoformat()
        end = date(self.bd.year, 12, 31).isoformat()
        tiers = "REWARDS_CASH:5000 PTS;TRAVEL_PREMIUM:7500 PTS;NO_FEE_BASIC:$25;BT_LOW_APR:$25"
        rows = [
            (
                "CMP_T01_ACT_01",
                "New account activation bonus",
                "T01_NEW_ACCOUNT_NOT_ACTIVATED",
                "ACT_BONUS_TIERED",
                tiers,
                "38.00",
                start,
                end,
                "CA-2027-0142",
                "ALL",
            ),
            (
                "CMP_T01_ACT_02",
                "Digital wallet provisioning nudge",
                "T01_NEW_ACCOUNT_NOT_ACTIVATED",
                "WALLET_PROVISION_NUDGE",
                "NONE",
                "0.15",
                start,
                end,
                "CA-2027-0143",
                "ALL",
            ),
            (
                "CMP_T02_RET_01",
                "Category boost on top spend category",
                "T02_SPEND_DECLINE",
                "RET_CATEGORY_BOOST",
                "3X POINTS 90 DAYS",
                "21.00",
                start,
                end,
                "CA-2027-0151",
                "REWARDS_CASH;TRAVEL_PREMIUM",
            ),
            (
                "CMP_T02_RET_02",
                "Retention bonus points",
                "T02_SPEND_DECLINE",
                "RET_BONUS_POINTS",
                "2500 PTS AFTER $500 SPEND",
                "25.00",
                start,
                end,
                "CA-2027-0152",
                "ALL",
            ),
        ]
        write_csv(root, "campaign", "campaign", self.bd, 1, CAMPAIGN_COLUMNS, rows)
        track("campaign.campaign", 1, len(rows))
        self._snap["campaign"] = rows

    def _write_transactions(self, root: Path, m: Manifest, track) -> None:
        cfg, bd_o = self.cfg, self.bd_o
        has_m = self.t_merch >= 0
        mi = np.maximum(self.t_merch, 0)
        cols = [
            self.t_ref,
            self.card_token[self.t_card],
            self.acct_no[self.t_acct],
            ymd(self.t_post_o),
            ymd(self.t_txn_o),
            np.char.zfill(
                (self.t_secs // 3600 * 10_000 + self.t_secs % 3600 // 60 * 100 + self.t_secs % 60).astype(str), 6
            ),
            self.t_amount.astype(str),
            np.full(len(self.t_ref), "USD"),
            np.where(has_m, self.merch_id[mi], ""),
            np.where(has_m, self.merch_name[mi], ""),
            np.where(has_m, np.array([m_[0] for m_ in ref.MCC_TABLE])[self.merch_mcc_idx[mi]], ""),
            self.t_type,
            self.t_entry,
            np.full(len(self.t_ref), ""),
            np.where(has_m, self.merch_tz[mi], "ET"),
        ]
        cols = [c.tolist() for c in cols]
        rows = [list(r) for r in zip(*cols, strict=True)]

        # Group rows into files: one daily file per past post date, intraday batches on the business date.
        keys = list(zip(self.t_post_o.tolist(), self.t_batch.tolist(), strict=True))
        files: dict[tuple[int, int], list[list[str]]] = {}
        for k, row in zip(keys, rows, strict=True):
            files.setdefault(k, []).append(row)

        defects = cfg.defects
        trailer_adjust: dict[tuple[int, int], int] = {}
        if defects.enabled:
            m.defects = self._inject_defects(files, defects, trailer_adjust)

        n_rows = 0
        for (post_o, seq), file_rows in sorted(files.items()):
            post_date = date.fromordinal(EPOCH.toordinal() + post_o)
            write_hdt(
                root,
                "processor",
                "posted_txn",
                "POSTED_TXN",
                post_date,
                seq,
                POSTED_TXN_COLUMNS,
                file_rows,
                amount_field="TXN_AMT",
                trailer_count_adjust=trailer_adjust.get((post_o, seq), 0),
            )
            n_rows += len(file_rows)
        track("processor.posted_txn", len(files), n_rows)
        m.rows["processor.posted_txn.business_date"] = sum(len(r) for (p, _), r in files.items() if p == bd_o)

    def _inject_defects(self, files, d, trailer_adjust) -> dict[str, object]:
        rng, bd_o = self.rng, self.bd_o
        bad = (bd_o, d.bad_trailer_batch) if d.bad_trailer_batch and (bd_o, d.bad_trailer_batch) in files else None
        candidates = [
            (k, i)
            for k, rows in files.items()
            if k[0] == bd_o and k != bad
            for i, r in enumerate(rows)
            if r[11] == "05"
        ]
        need = d.clear_pan_rows + d.pan_in_merchant_name_rows + d.malformed_rows + d.duplicate_rows + d.unknown_mcc_rows
        pick = rng.choice(len(candidates), size=min(need, len(candidates)), replace=False)
        chosen = [candidates[i] for i in pick]
        out: dict[str, object] = {"quarantine_expected": {}, "rejected_files": []}
        q = out["quarantine_expected"]
        it = iter(chosen)
        for j in range(d.clear_pan_rows):
            k, i = next(it)
            files[k][i][1] = ref.TEST_PANS[j % len(ref.TEST_PANS)]
        q["PAN_DETECTED"] = d.clear_pan_rows
        for _ in range(d.pan_in_merchant_name_rows):
            k, i = next(it)
            files[k][i][9] = "PMT REF CARD 4111-1111-1111-1111"
        q["PAN_DETECTED"] += d.pan_in_merchant_name_rows
        for _ in range(d.malformed_rows):
            k, i = next(it)
            files[k][i][9] = files[k][i][9] + "|EXTRA"
        q["FIELD_COUNT"] = d.malformed_rows
        dups = [next(it) for _ in range(d.duplicate_rows)]
        for k, i in dups:
            files[k].append(list(files[k][i]))
        out["duplicate_rows_in_bronze"] = d.duplicate_rows
        for _ in range(d.unknown_mcc_rows):
            k, i = next(it)
            files[k][i][10] = "0001"
        out["unknown_mcc_rows"] = d.unknown_mcc_rows
        if bad:
            trailer_adjust[bad] = 1
            out["rejected_files"] = [
                f"processor_posted_txn_{date.fromordinal(EPOCH.toordinal() + bad[0]):%Y%m%d}_{bad[1]:04d}.dat"
            ]
            out["rows_in_rejected_files"] = len(files[bad])
        return out


def generate(cfg: SynthConfig, landing_root: Path) -> Manifest:
    return SyntheticBank(cfg).build().write(landing_root)
