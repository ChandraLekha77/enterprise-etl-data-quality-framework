"""Create idempotent GX assets, batch definitions, validations, and checkpoint."""

from __future__ import annotations

from pathlib import Path

import great_expectations as gx


ROOT = Path(__file__).resolve().parents[1]
GX_ROOT = ROOT / "great_expectations"
DATA_SOURCE = "warehouse"
ASSETS = {
    "transaction_candidates": "select * from core.int_transaction_candidates",
    "operational_event_candidates": "select * from core.int_operational_event_candidates",
}
SUITES = {
    "transaction_candidates": "transaction_quality",
    "operational_event_candidates": "operational_event_quality",
}


def configure() -> None:
    context = gx.get_context(context_root_dir=str(GX_ROOT))
    data_source = context.data_sources.get(DATA_SOURCE)
    existing_assets = {asset.name for asset in data_source.assets}
    definitions = []
    for asset_name, query in ASSETS.items():
        asset = (
            data_source.get_asset(asset_name)
            if asset_name in existing_assets
            else data_source.add_query_asset(name=asset_name, query=query)
        )
        try:
            batch_definition = asset.get_batch_definition("whole_candidate_set")
        except KeyError:
            batch_definition = asset.add_batch_definition_whole_table(
                name="whole_candidate_set"
            )
        suite = context.suites.get(name=SUITES[asset_name])
        definition = gx.ValidationDefinition(
            name=f"validate_{asset_name}", data=batch_definition, suite=suite
        )
        definitions.append(context.validation_definitions.add_or_update(definition))

    checkpoint = gx.Checkpoint(
        name="warehouse_quality",
        validation_definitions=definitions,
        result_format="SUMMARY",
    )
    context.checkpoints.add_or_update(checkpoint)


if __name__ == "__main__":
    configure()
