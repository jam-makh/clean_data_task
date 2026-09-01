"""
The run manifest and data-quality report.

This is where the pipeline diagnostics live now. The feature table carries
modelling features; this file carries everything else the run knows about
itself -- what was excluded and why, what was carried forward, what could not
be classified, and how long each of it took.

Every entry explains itself. A metric is not a bare number but an object with
``value``, ``what`` it counts and what it ``means``, and counts carry the
denominator they are a fraction of, because "18,432 carried forward" says
nothing until you know whether that is out of twenty thousand or two million.
The alternative -- a number here and its documentation in a README -- is a
pair that drifts.
"""

from features import contract
from features import source
from features.settings import FeatureSettings
from src.rules.store import Rules


def metric(value, what: str, means: str, of=None) -> dict:
    """
    One self-describing entry of the report.

    :param value: The number itself.
    :param what: What it counts, in one sentence.
    :param means: How to read it -- what a high or low value tells you.
    :param of: The denominator, where the value is a count out of something.
    :returns: The entry.
    """
    entry = {"value": value, "what": what, "means": means}
    if of is not None:
        entry["of"] = of
        entry["pct"] = (
            round(100.0 * float(value) / float(of), 4)
            if of and value is not None
            else None
        )
    return entry


def _balance_quality(
    txns: dict, months: dict, users: dict, config: FeatureSettings
) -> dict:
    """
    :param txns: Transaction-level diagnostics.
    :param months: Account-month diagnostics.
    :param users: User-month diagnostics.
    :param config: The build settings, whose eligibility list is echoed.
    :returns: The balance section of the report.
    """
    total_txns = txns["txns_total"]
    account_months = months["account_months_total"]
    user_months = users["user_months_total"]

    return {
        "eligible_statuses": list(config.balance.eligible_statuses),
        "rows_by_balance_status": metric(
            txns["rows_by_balance_status"],
            "Transactions per running_balance_status across the whole "
            "extract.",
            "The shape of the evidence before any threshold is applied. This "
            "is the distribution the eligibility list is a judgement about.",
        ),
        "rows_excluded_contradicted": metric(
            txns["rows_excluded_contradicted"],
            "Transactions whose running balance was reconstructed two ways "
            "and the two disagree.",
            "Balances discarded as untrustworthy. The largest single "
            "exclusion in Stage 3, and the reason the eligible list is "
            "configuration rather than a constant: publishing one of two "
            "answers is worse than publishing none.",
            of=total_txns,
        ),
        "rows_excluded_unavailable": metric(
            txns["rows_excluded_unavailable"],
            "Transactions carrying no running balance at all.",
            "Not a judgement call, simply absence. Held apart from "
            "CONTRADICTED because the two fail for different reasons and a "
            "combined number would hide which.",
            of=total_txns,
        ),
        "rows_excluded_by_status_total": metric(
            txns["rows_excluded_by_status_total"],
            "Transactions whose status is outside eligible_statuses.",
            "The full transaction-level cost of the eligibility rule. Move a "
            "status into the list above and this is the number that moves.",
            of=total_txns,
        ),
        "rows_eligible_but_null": metric(
            txns["rows_eligible_but_null"],
            "Transactions with an eligible status whose normalized balance "
            "is nonetheless null.",
            "Should be zero. Anything else means Stage 2's status column and "
            "its value column disagree -- an upstream contract violation, "
            "not a Stage 3 problem.",
            of=total_txns,
        ),
        "account_months_total": metric(
            account_months,
            "Rows on the dense account-by-month spine.",
            "The denominator for the three counts below. Larger than the "
            "number of active account-months by design: an account that goes "
            "quiet keeps producing rows, because its balance persists.",
        ),
        "account_months_observed": metric(
            months["account_months_observed"],
            "Account-months that stated an eligible balance of their own.",
            "Fresh evidence. The share of the spine resting on a figure the "
            "source actually wrote in that month.",
            of=account_months,
        ),
        "account_months_carried_forward": metric(
            months["account_months_carried_forward"],
            "Account-months with no observation of their own, filled from an "
            "earlier month.",
            "Real figures, but stale. A high rate means the balance series "
            "leans on persistence rather than on fresh statements, which "
            "matters most to the month-on-month delta features.",
            of=account_months,
        ),
        "account_months_without_balance": metric(
            months["account_months_without_balance"],
            "Account-months before the account's first eligible balance, "
            "with nothing to carry.",
            "Genuinely unknown, not zero. Nothing is ever filled backwards: "
            "inventing a figure here would put a number where the source has "
            "none.",
            of=account_months,
        ),
        "max_carry_forward_run_months": metric(
            months["max_carry_forward_run_months"],
            "The longest unbroken stretch a single balance was held forward.",
            "How stale the stalest figure gets. A twenty-month run means one "
            "number is standing in for nearly two years of an account.",
        ),
        "accounts_total": metric(
            months["accounts_total"],
            "Distinct accounts on the spine.",
            "The size of the account layer that the user rollup sums over.",
        ),
        "accounts_never_with_balance": metric(
            months["accounts_never_with_balance"],
            "Accounts that never once supplied an eligible balance.",
            "These contribute rows to the spine and nothing to any user "
            "total. They are the source of the partial rollups below.",
            of=months["accounts_total"],
        ),
        "user_months_partial_rollup": metric(
            users["user_months_partial_rollup"],
            "User-months where fewer accounts supplied a balance than the "
            "user held on the spine.",
            "The user's total is a sum over some of their accounts, not all "
            "of them. It looks like a decline in the balance series and is "
            "not one -- which is what the removed accounts_contributing and "
            "accounts_with_balance columns existed to expose.",
            of=user_months,
        ),
        "user_months_carried_forward": metric(
            users["user_months_carried_forward"],
            "User-months where at least one contributing account's balance "
            "was carried rather than observed.",
            "The user-grain view of staleness, and the replacement for the "
            "removed balance_is_carried_forward flag.",
            of=user_months,
        ),
        "user_months_without_balance": metric(
            users["user_months_without_balance"],
            "User-months where no account supplied a balance, so the target "
            "is null.",
            "Rows Stage 4 cannot train on. The honest count of unusable "
            "label rows, reported rather than filled with a zero.",
            of=user_months,
        ),
    }


def _direction_quality(txns: dict, users: dict) -> dict:
    """
    :param txns: Transaction-level diagnostics.
    :param users: User-month diagnostics.
    :returns: The declared-direction section of the report.
    """
    total = txns["txns_total"]
    return {
        "txns_total": metric(
            total,
            "Cleaned transactions read by this build.",
            "The denominator for this section, and the input size the "
            "timings below should be read against.",
        ),
        "txns_credit": metric(
            txns["txns_credit"],
            "Transactions whose processing code declares CREDIT.",
            "Money in. Enters total_credited_usd and nothing else.",
            of=total,
        ),
        "txns_debit": metric(
            txns["txns_debit"],
            "Transactions whose processing code declares DEBIT.",
            "Money out. Enters total_debited_usd, and enters a spending "
            "category only if the code is also spend-eligible.",
            of=total,
        ),
        "txns_undeclared_direction": metric(
            txns["txns_undeclared_direction"],
            "Transactions whose processing code declares no direction.",
            "Counted in neither flow total and therefore absent from net "
            "flow. This is the one number that explains a net flow which "
            "does not reconcile against the balance change.",
            of=total,
        ),
        "undeclared_amount_usd": metric(
            txns["undeclared_amount_usd"],
            "Absolute USD magnitude of those undeclared transactions.",
            "How much money is missing from the flow totals, not merely how "
            "many rows. A hundred tiny rows and a hundred large ones are "
            "different problems and the row count cannot tell them apart.",
        ),
        "undeclared_by_code": metric(
            txns["undeclared_by_code"],
            "Undeclared transactions broken down by processing code.",
            "Names the codes to fix. One dominant code is a one-line "
            "addition to rule_processing_codes; a long tail is a source "
            "problem that Stage 2 should be looking at.",
        ),
        "user_months_with_undeclared": metric(
            users["user_months_with_undeclared"],
            "User-months containing at least one undeclared transaction.",
            "How far the contamination spreads. Concentrated in a few users "
            "is a very different situation from one row in every month.",
            of=users["user_months_total"],
        ),
        "sign_disagreements": metric(
            txns["sign_disagreements"],
            "Transactions whose stated amount sign contradicts the direction "
            "their code declares.",
            "Zero on the current extract. The day it is not, the source has "
            "changed its sign convention, and because Stage 3 takes the "
            "magnitude and the direction from different places, the flow "
            "totals are the last thing that would show it.",
            of=total,
        ),
    }


def _spending_quality(txns: dict, users: dict, rules: Rules) -> dict:
    """
    :param txns: Transaction-level diagnostics.
    :param users: User-month diagnostics.
    :param rules: The vocabularies.
    :returns: The spending section of the report.
    """
    eligible = txns["txns_spend_eligible"]
    return {
        "categories": list(rules.categories),
        "residual_category": rules.residual,
        "txns_spend_eligible": metric(
            eligible,
            "Debits whose processing code counts as spending.",
            "The population the category amount columns are built from. "
            "Smaller than the debit count, deliberately.",
            of=txns["txns_total"],
        ),
        "txns_debit_not_spend_eligible": metric(
            txns["txns_debit_not_spend_eligible"],
            "Debits excluded from spending -- transfers, settlements, "
            "internal movements.",
            "Real outflows that belong in total_debited_usd and in no "
            "category. This is why the category amounts sum to less than the "
            "debit total, and it is correct that they do.",
            of=txns["txns_debit"],
        ),
        "spend_rows_unmapped_mcc": metric(
            txns["spend_rows_unmapped_mcc"],
            "Spend-eligible transactions whose MCC is absent from the "
            "category map.",
            f"Routed to {rules.residual!r} rather than dropped, so the "
            f"category amounts still sum to the total. A rising count means "
            f"the residual is absorbing categories that deserve their own "
            f"column.",
            of=eligible,
        ),
        "spend_rows_null_mcc": metric(
            txns["spend_rows_null_mcc"],
            "Spend-eligible transactions carrying no MCC at all.",
            f"Also routed to {rules.residual!r}. Held apart from the "
            f"unmapped count because one is a gap in our map and the other a "
            f"gap in the data.",
            of=eligible,
        ),
        "unmapped_mcc_top": metric(
            txns["unmapped_mcc_top"],
            "The most common unmapped MCCs, with their transaction counts.",
            "The shortlist for extending rule_mcc_categories, in the order "
            "that would reduce the residual fastest.",
        ),
        "residual_share_of_spend": metric(
            users["residual_share_of_spend"],
            f"The {rules.residual!r} category as a fraction of all spend in "
            f"this run.",
            "The one share worth publishing, and it belongs here rather than "
            "as a column repeated on every row. Above roughly 0.3 the "
            "category split is not saying much and the MCC map needs work.",
        ),
        "total_spend_usd": metric(
            users["total_spend_usd"],
            "All spend-eligible debits in this run, in USD.",
            "The denominator of the share above, and a cross-check that the "
            "category amounts add up.",
        ),
        "user_months_zero_spend": metric(
            users["user_months_zero_spend"],
            "User-months with no spend-eligible activity.",
            "Rows where every category amount is a true zero rather than a "
            "gap. Stage 4 should expect these; the dense spine is why they "
            "exist.",
            of=users["user_months_total"],
        ),
    }


def _activity_quality(txns: dict, users: dict) -> dict:
    """
    :param txns: Transaction-level diagnostics.
    :param users: User-month diagnostics.
    :returns: The activity and dormancy section of the report.
    """
    user_months = users["user_months_total"]
    return {
        "user_months_inactive": metric(
            users["user_months_inactive"],
            "User-months with no transactions at all.",
            "How much of the table is dense-spine padding rather than "
            "observed activity. Flows and counts are true zeros on these "
            "rows and the balance is whatever was last known.",
            of=user_months,
        ),
        "max_months_since_last_txn": metric(
            users["max_months_since_last_txn"],
            "The longest dormancy anywhere in the table, in months.",
            "The worst case in one number, which is what the removed "
            "prev_1m_months_since_last_txn column was repeating on every "
            "row.",
        ),
        "months_since_last_txn_p50": metric(
            users["months_since_last_txn_p50"],
            "Median months since a user's last transaction.",
            "Typical staleness. Read with p90 below: a low median and a high "
            "p90 means long gaps are a tail rather than the norm.",
        ),
        "months_since_last_txn_p90": metric(
            users["months_since_last_txn_p90"],
            "90th percentile of the same gap.",
            "Where the tail sits. This is the number that should shape how "
            "Stage 4 treats quiet months.",
        ),
        "internal_descriptor_rows": metric(
            txns["internal_descriptor_rows"],
            "Transactions whose merchant name is an internal movement label "
            "rather than a counterparty.",
            "Excluded from the distinct-merchant count. A user with three "
            "standing orders is not a user shopping at three merchants.",
            of=txns["txns_total"],
        ),
        "rows_without_parseable_month": metric(
            txns["rows_without_parseable_month"],
            "Source rows with no usable txn_ts, and therefore no month.",
            "Rows that contribute to no month and leave the build silently. "
            "Should be zero after Stage 2; reported so that it is visibly "
            "zero rather than merely assumed.",
            of=txns["txns_total"],
        ),
    }


def build(
    table,
    rules: Rules,
    config: FeatureSettings,
    txns: dict,
    months: dict,
    users: dict,
    timings,
    rows_written: int | None,
) -> dict:
    """
    Assembles the whole manifest.

    :param table: The projected feature table, for its schema.
    :param rules: The vocabularies.
    :param config: The build settings.
    :param txns: Transaction-level diagnostics.
    :param months: Account-month diagnostics.
    :param users: User-month diagnostics.
    :param timings: Per-phase durations and JVM peak memory.
    :param rows_written: Rows the upsert touched, or None if it was skipped.
    :returns: The manifest, ready to serialise.
    """
    declared = contract.columns(rules.categories)

    return {
        "grain": "one row per user_id per month",
        "destination": {
            "table": config.database.table,
            "rows_written": rows_written,
            "note": "The feature table is written to Postgres and nowhere "
                    "else. Stage 4 reads this table directly; there is no "
                    "file artifact to keep in step with it.",
        },
        "coverage": {
            "source_rows": metric(
                txns["txns_total"],
                "Cleaned transactions read.",
                "The input size. Every timing below is a time to process "
                "this many rows.",
            ),
            "feature_rows": metric(
                users["user_months_total"],
                "Rows in the feature table, at one row per user per month.",
                "Larger than the number of active user-months, because the "
                "spine is dense: a quiet month is an observation.",
            ),
            "users": metric(
                users["users"],
                "Distinct users in the table.",
                "The number of series Stage 4 has to model.",
            ),
            "months": {
                "first": users["first_month"],
                "last": users["last_month"],
                "count": users["months"],
            },
        },
        "units": {
            "money": "USD",
            "flow_column": "billing_amount (absolute), signed by rule",
            "balance_column": "running_balance_normalized",
            "never_read": list(source.FORBIDDEN),
            "note": "Every monetary column is USD. The native-currency "
                    "columns are listed so the ban on reading them is "
                    "testable rather than a matter of remembering.",
        },
        "rules": {
            "spend_eligible_codes": sorted(rules.spend_eligible),
            "spending_categories": list(rules.categories),
            "residual_category": rules.residual,
            "mapped_mccs": len(rules.mcc_categories),
        },
        "balance_quality": _balance_quality(txns, months, users, config),
        "direction_quality": _direction_quality(txns, users),
        "spending_quality": _spending_quality(txns, users, rules),
        "activity_quality": _activity_quality(txns, users),
        "point_in_time": {
            "rule": "Every BEFORE_MONTH column is computed from months "
                    "strictly earlier than the row's own. CALENDAR columns "
                    "read the row's month legitimately: both are fixed "
                    "before it begins. The TARGET reads month M and is never "
                    "an input.",
            "known_at": {
                column.name: column.known_at for column in declared
            },
            "target": "target_closing_balance_usd",
            "features": contract.feature_names(rules.categories),
        },
        "schema": {
            column.name: {
                "kind": column.kind,
                "known_at": column.known_at,
                "note": column.note,
            }
            for column in declared
        },
        "performance": {
            "phase_seconds": timings.phases,
            "total_seconds": timings.total,
            "slowest_phase": timings.slowest,
            "jvm_peak_memory_mb": timings.jvm_peak_mb,
            "driver_peak_memory_mb": timings.driver_peak_mb,
            "note": "Spark is lazy, so a phase is timed by materialising at "
                    "its boundary rather than by timing the call that builds "
                    "the plan. Peak memory is the JVM's, because that is "
                    "where the work happens; the driver figure is the Python "
                    "process and is expected to be small.",
        },
    }
