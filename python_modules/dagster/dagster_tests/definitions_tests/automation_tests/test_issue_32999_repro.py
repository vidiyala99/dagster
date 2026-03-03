import datetime
import logging

import dagster as dg
from dagster import AutomationCondition as AC
from dagster._core.definitions.asset_daemon_cursor import AssetDaemonCursor
from dagster._core.definitions.asset_key import AssetKey
from dagster._core.definitions.declarative_automation.automation_condition_evaluator import (
    AutomationConditionEvaluator,
)
from dagster._core.definitions.declarative_automation.automation_condition_tester import (
    EvaluateAutomationConditionsResult,
)
from dagster._core.definitions.partitions.context import partition_loading_context


async def evaluate_automation_conditions_async(
    defs,
    instance,
    asset_selection=None,
    evaluation_time=None,
    cursor=None,
):
    if not isinstance(defs, dg.Definitions):
        defs = dg.Definitions(assets=defs)
    if asset_selection is None:
        asset_selection = (
            dg.AssetSelection.all(include_sources=True) | dg.AssetSelection.all_asset_checks()
        )
    asset_graph = defs.resolve_asset_graph()
    cursor = cursor or AssetDaemonCursor.empty()
    evaluator = AutomationConditionEvaluator(
        asset_graph=asset_graph,
        instance=instance,
        entity_keys={
            key
            for key in asset_selection.resolve(asset_graph)
            | asset_selection.resolve_checks(asset_graph)
            if asset_graph.get(key).automation_condition is not None
        },
        evaluation_time=evaluation_time,
        emit_backfills=False,
        logger=logging.getLogger("dagster.automation_condition_tester"),
        cursor=cursor,
        evaluation_id=cursor.evaluation_id,
    )
    results, requested_subsets = await evaluator.async_evaluate()
    with partition_loading_context(
        effective_dt=evaluation_time, dynamic_partitions_store=instance
    ) as ctx:
        new_cursor = cursor.with_updates(
            evaluation_timestamp=(evaluation_time or datetime.datetime.now()).timestamp(),
            newly_observe_requested_asset_keys=[],
            evaluation_id=cursor.evaluation_id + 1,
            condition_cursors=[result.get_new_cursor() for result in results],
            asset_graph=asset_graph,
        )
        return EvaluateAutomationConditionsResult(
            cursor=new_cursor,
            requested_subsets=requested_subsets,
            results=results,
            partition_loading_context=ctx,
        )


def test_issue_32999_id_collision_resolved() -> None:
    """Verifies that the ID collision is resolved after the fix."""
    asset_a = AssetKey("asset_a")
    asset_b = AssetKey("asset_b")
    condition = (
        AC.any_deps_match(AC.data_version_changed())
        .allow(dg.AssetSelection.assets(asset_a))
        .since_last_handled()
        & AC.any_deps_match(AC.data_version_changed())
        .allow(dg.AssetSelection.assets(asset_b))
        .since_last_handled()
    )
    snapshot = condition.get_snapshot()
    since_a = snapshot.children[0]
    since_b = snapshot.children[1]
    any_deps_a = since_a.children[0]
    any_deps_b = since_b.children[0]

    # POST-FIX: These must NOT be equal, as they track different assets
    assert since_a.node_snapshot.unique_id != since_b.node_snapshot.unique_id
    assert any_deps_a.node_snapshot.unique_id != any_deps_b.node_snapshot.unique_id


def test_issue_32999_backcompat() -> None:
    """Verifies that the old (colliding) ID is still present in the backcompat list."""
    asset_a = AssetKey("asset_a")
    condition = AC.any_deps_match(AC.data_version_changed()).allow(
        dg.AssetSelection.assets(asset_a)
    )

    # This is the ID that would have been generated before the fix
    # (where allow_selection was ignored)
    old_id = "80f87fb32baaf7ce3f65f68c12d3eb11"

    backcompat_ids = condition.get_backcompat_node_unique_ids(
        parent_unique_id=None, index=None, target_key=None
    )
    assert old_id in backcompat_ids
