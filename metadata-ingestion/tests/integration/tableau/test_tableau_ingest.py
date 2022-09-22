import pytest
import test_tableau_common
from freezegun import freeze_time
from typing import Optional, cast


from datahub.configuration.source_common import DEFAULT_ENV
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.tableau import TableauSource
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.tableau_common import (
    TableauLineageOverrides,
    make_table_urn,
)
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)
from tests.test_helpers.state_helpers import validate_all_providers_have_committed_successfully


FROZEN_TIME = "2021-12-07 07:00:00"

test_resources_dir = None


@freeze_time(FROZEN_TIME)
@pytest.mark.slow_unit
def test_tableau_ingest(pytestconfig, tmp_path):
    output_file_name: str = "tableau_mces.json"
    golden_file_name: str = "tableau_mces_golden.json"
    side_effect_query_metadata = test_tableau_common.define_query_metadata_func(
        "workbooksConnection_0.json", "workbooksConnection_all.json"
    )
    test_tableau_common.tableau_ingest_common(
        pytestconfig,
        tmp_path,
        side_effect_query_metadata,
        golden_file_name,
        output_file_name,
    )


def test_lineage_overrides():
    # Simple - specify platform instance to presto table
    assert (
        make_table_urn(
            DEFAULT_ENV,
            "presto_catalog",
            "presto",
            "test-schema",
            "presto_catalog.test-schema.test-table",
            platform_instance_map={"presto": "my_presto_instance"},
        )
        == "urn:li:dataset:(urn:li:dataPlatform:presto,my_presto_instance.presto_catalog.test-schema.test-table,PROD)"
    )

    # Transform presto urn to hive urn
    # resulting platform instance for hive = mapped platform instance + presto_catalog
    assert (
        make_table_urn(
            DEFAULT_ENV,
            "presto_catalog",
            "presto",
            "test-schema",
            "presto_catalog.test-schema.test-table",
            platform_instance_map={"presto": "my_instance"},
            lineage_overrides=TableauLineageOverrides(
                platform_override_map={"presto": "hive"},
            ),
        )
        == "urn:li:dataset:(urn:li:dataPlatform:hive,my_instance.presto_catalog.test-schema.test-table,PROD)"
    )

    # tranform hive urn to presto urn
    assert (
        make_table_urn(
            DEFAULT_ENV,
            "",
            "hive",
            "test-schema",
            "test-schema.test-table",
            platform_instance_map={"hive": "my_presto_instance.presto_catalog"},
            lineage_overrides=TableauLineageOverrides(
                platform_override_map={"hive": "presto"},
            ),
        )
        == "urn:li:dataset:(urn:li:dataPlatform:presto,my_presto_instance.presto_catalog.test-schema.test-table,PROD)"
    )

def get_current_checkpoint_from_pipeline(
    pipeline: Pipeline,
) -> Optional[Checkpoint]:
    tableau_source = cast(TableauSource, pipeline.source)
    return tableau_source.get_current_checkpoint(
        tableau_source.stale_entity_removal_handler.job_id
    )

@freeze_time(FROZEN_TIME)
def test_tableau_stateful(pytestconfig, tmp_path, mock_time, mock_datahub_graph):
    output_file_name: str = "tableau_mces.json"
    golden_file_name: str = "tableau_mces_golden.json"
    side_effect_query_metadata = [
        test_tableau_common.define_query_metadata_func(
            "workbooksConnection_0.json", "workbooksConnection_all.json"
        ),
        test_tableau_common.define_query_metadata_func(
            "workbooksConnection_0_stateful.json", "workbooksConnection_all.json"
        )
    ]
    pipeline_run1= test_tableau_common.tableau_ingest_common(
        pytestconfig,
        tmp_path,
        side_effect_query_metadata,
        golden_file_name,
        output_file_name,
    )

    checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)
    assert checkpoint1
    assert checkpoint1.state

    pipeline_run2 = test_tableau_common.tableau_ingest_common(
        pytestconfig,
        tmp_path,
        side_effect_query_metadata,
        golden_file_name,
        output_file_name,
    )
    checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)

    assert checkpoint2
    assert checkpoint2.state

    # Validate that all providers have committed successfully.
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline_run1, expected_providers=1
    )
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline_run2, expected_providers=1
    )

    # Perform all assertions on the states. The deleted table should not be
    # part of the second state
    state1 = cast(BaseSQLAlchemyCheckpointState, checkpoint1.state)
    state2 = cast(BaseSQLAlchemyCheckpointState, checkpoint2.state)
    difference_urns = list(
        state1.get_urns_not_in(type="table", other_checkpoint_state=state2)
    )

    assert len(difference_urns) == 1

    urn1 = (
        "urn:li:dataset:(urn:li:dataPlatform:glue,flights-database.avro,PROD)"
    )

    assert urn1 in difference_urns