"""M7: deploy artifacts — role dispatch, Helm chart, Terraform, Dockerfile."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from claimpipe.__main__ import VALID_ROLES, run

ROOT = Path(__file__).resolve().parents[1]


def test_valid_roles() -> None:
    assert VALID_ROLES == {"api", "worker", "relay", "notifier"}


def test_unknown_role_exits() -> None:
    with pytest.raises(SystemExit):
        run("bogus-role")


def test_helm_chart_wellformed() -> None:
    chart = yaml.safe_load((ROOT / "charts/claimpipe/Chart.yaml").read_text())
    assert chart["name"] == "claimpipe"

    values = yaml.safe_load((ROOT / "charts/claimpipe/values.yaml").read_text())
    # every dispatchable role is a deployable role in the chart
    assert set(values["roles"]) == VALID_ROLES
    # api role exposes a port; the config carries the adapter endpoints
    assert values["roles"]["api"]["port"] == 8000
    assert "CLAIMPIPE_TEMPORAL_ADDRESS" in values["config"]
    assert "CLAIMPIPE_KAFKA_BOOTSTRAP" in values["config"]


def test_deploy_files_present() -> None:
    assert (ROOT / "Dockerfile").exists()
    for f in ("main.tf", "variables.tf", "outputs.tf"):
        assert (ROOT / "deploy/terraform" / f).exists()
