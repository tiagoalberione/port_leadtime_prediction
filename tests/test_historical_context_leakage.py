"""Testes permanentes de anti-leakage das features historicas seguras."""

from src.features.historical_context import run_leakage_tests


def test_historical_context_leakage_guards_pass():
    """Garante que os cenarios adversariais de disponibilidade temporal passam."""
    results = run_leakage_tests()
    assert results["passed"].all(), results.loc[~results["passed"]].to_dict("records")
