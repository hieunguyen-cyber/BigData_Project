import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "data-generator"))
sys.path.insert(0, str(ROOT / "scripts"))

from scenarios import SCENARIOS, generate  # noqa: E402
import check_simulator_plausibility as checker  # noqa: E402


def _run_checker(tmp_path, scenario):
    events = generate(scenario, seed=42, rate=20, duration=60)
    output = tmp_path / f"{scenario}.jsonl"
    output.write_text("\n".join(__import__("json").dumps(event) for event in events) + "\n", encoding="utf-8")
    return checker.main(["--file", str(output), "--expect", scenario])


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_all_simulator_scenarios_pass_plausibility_checker(tmp_path, scenario):
    assert _run_checker(tmp_path, scenario) == 0


def test_same_seed_generates_identical_output():
    assert generate("velocity_fraud", seed=9) == generate("velocity_fraud", seed=9)
