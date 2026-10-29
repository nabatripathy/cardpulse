"""Data quality rules engine: rules declared in YAML, evaluated in DuckDB (Phase 4 DQ rules)."""

from .rules import DatasetRuleResult, Rule, RuleResult, evaluate_dataset_rules, evaluate_row_rules

__all__ = ["DatasetRuleResult", "Rule", "RuleResult", "evaluate_dataset_rules", "evaluate_row_rules"]
