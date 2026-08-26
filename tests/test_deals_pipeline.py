from dataclasses import asdict
from unittest.mock import MagicMock, patch

import pytest

from source_zoho_crm.streams import IncrementalZohoCrmStream, ZohoCrmStream
from source_zoho_crm.types import (
    DEALS_MANDATORY_RECORD_FIELDS,
    FieldMeta,
    ModuleMeta,
    ZOHO_RECORDS_MAX_FIELDS_PER_REQUEST,
    build_deals_fallback_schema,
    build_record_field_batches,
    collect_module_record_field_names,
)


CONFIG = {"start_datetime": "2020-01-01T00:00:00+00:00"}


def _field(api_name: str) -> dict:
    return {
        "json_type": "string",
        "length": 255,
        "api_name": api_name,
        "data_type": "text",
        "decimal_place": None,
        "system_mandatory": False,
        "display_label": api_name,
        "pick_list_values": [],
    }


def _deals_stream(field_count: int = 0) -> IncrementalZohoCrmStream:
    fields = [FieldMeta.from_dict(_field(f"Custom_Field_{index}")) for index in range(field_count)]
    module = ModuleMeta(api_name="Deals", module_name="Deals", api_supported=True, fields=fields)
    stream_cls = type(
        "IncrementalDealsZohoCRMStream",
        (IncrementalZohoCrmStream,),
        {"url_base": "https://www.zohoapis.eu", "module": module},
    )
    return stream_cls(authenticator=None, config=CONFIG)


def test_fallback_deals_schema_contains_pipeline() -> None:
    schema = asdict(build_deals_fallback_schema())

    assert "Pipeline" in schema["properties"]
    assert "Pipeline" in schema["required"]
    assert "Stage" in schema["required"]
    assert "Owner" in schema["required"]


def test_deals_fields_request_contains_pipeline() -> None:
    stream = _deals_stream(field_count=0)
    batches = stream._field_request_batches()

    assert batches
    assert any("Pipeline" in batch for batch in batches)
    assert stream.path() == "/crm/v8/Deals"


def test_deals_request_params_include_pipeline_in_fields() -> None:
    stream = _deals_stream(field_count=2)
    stream._active_field_batch = stream._field_request_batches()[0]

    params = stream.request_params({})

    assert "fields" in params
    assert "Pipeline" in params["fields"].split(",")
    assert params["page"] == 1
    assert params["per_page"] == 200


def test_pipeline_survives_batching_over_50_fields() -> None:
    stream = _deals_stream(field_count=60)
    batches = stream._field_request_batches()

    assert len(batches) > 1
    assert any("Pipeline" in batch for batch in batches)
    assert all(len(batch) <= ZOHO_RECORDS_MAX_FIELDS_PER_REQUEST for batch in batches)
    assert all("id" in batch for batch in batches)


def test_pipeline_value_is_preserved_when_merging_batches() -> None:
    stream = _deals_stream(field_count=60)
    batch_one_records = [{"id": "1", "Stage": "Qualification", "Custom_Field_0": "A"}]
    batch_two_records = [{"id": "1", "Pipeline": "Діагностика", "Custom_Field_40": "B"}]

    with patch(
        "source_zoho_crm.streams.HttpStream.read_records",
        side_effect=[iter(batch_one_records), iter(batch_two_records)],
    ):
        result = list(stream.read_records())

    assert len(result) == 1
    assert result[0]["Pipeline"] == "Діагностика"
    assert result[0]["Stage"] == "Qualification"
    assert result[0]["Custom_Field_0"] == "A"
    assert result[0]["Custom_Field_40"] == "B"


def test_module_schema_with_metadata_includes_pipeline_for_deals() -> None:
    module = ModuleMeta(
        api_name="Deals",
        module_name="Deals",
        api_supported=True,
        fields=[FieldMeta.from_dict(_field("Stage"))],
    )
    schema = asdict(module.schema)

    assert "Pipeline" in schema["properties"]


def test_collect_module_record_field_names_includes_mandatory_deals_fields() -> None:
    module = ModuleMeta(api_name="Deals", module_name="Deals", api_supported=True, fields=[])

    names = collect_module_record_field_names(module)

    for field_name in DEALS_MANDATORY_RECORD_FIELDS:
        assert field_name in names


def test_build_record_field_batches_keeps_mandatory_fields_in_first_batch() -> None:
    field_names = [f"Field_{index}" for index in range(55)]
    batches = build_record_field_batches(field_names, DEALS_MANDATORY_RECORD_FIELDS)

    assert "Pipeline" in batches[0]
    assert "Stage" in batches[0]
