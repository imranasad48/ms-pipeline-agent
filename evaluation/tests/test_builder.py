import copy

import pytest

from evaluation import builder, settings as S

BASE = {"seed": 1, "end_date": "today", "history_days": 90, "guarantee_min_occurrences": False,
        "output": {"clean_baseline": True, "data_dir": "data"},
        "failures": {"a": {"enabled": True, "rate": 0.5}, "b": {"enabled": True},
                     "domain_c": {"enabled": True}}}


def test_only_requested_injector_enabled():
    cfg = builder.make_config(BASE, "b", 42, "2026-11-11")
    assert {k: v["enabled"] for k, v in cfg["failures"].items()} == {"a": False, "b": True, "domain_c": False}


def test_seed_end_date_guarantee_and_no_clean_baseline():
    cfg = builder.make_config(BASE, "a", 42, "2026-11-11")
    assert cfg["seed"] == 42 and cfg["end_date"] == "2026-11-11"
    assert cfg["guarantee_min_occurrences"] is True
    assert cfg["output"]["clean_baseline"] is False
    assert cfg["failures"]["a"]["rate"] == 0.5          # other knobs untouched


def test_base_config_not_mutated():
    snap = copy.deepcopy(BASE)
    builder.make_config(BASE, "a", 1, "2026-01-01")
    assert BASE == snap


def test_unknown_type_rejected():
    with pytest.raises(ValueError):
        builder.make_config(BASE, "nope", 1, "2026-01-01")
    with pytest.raises(SystemExit):
        builder.resolve_types("a,nope", BASE)


def test_resolve_all_and_list():
    assert builder.resolve_types("all", BASE) == ["a", "b", "domain_c"]
    assert builder.resolve_types("b, a", BASE) == ["b", "a"]


def test_scenario_dir_layout(tmp_path):
    assert builder.scenario_dir("a", 7, tmp_path) == tmp_path / "a" / "7"


@pytest.mark.skipif(not (S.GENERATOR_DIR / "config.yaml").exists(), reason="generator not present")
def test_real_generator_has_27_types_all_mapped():
    from evaluation.scorer import load_mapping
    types = builder.all_failure_types()
    assert len(types) == 27
    assert set(types) == set(load_mapping()["types"])
