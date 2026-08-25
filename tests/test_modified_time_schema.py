from dataclasses import asdict
from unittest.mock import MagicMock, patch

import pytest

from source_zoho_crm.streams import IncrementalZohoCrmStream, ZohoCrmStream, ZohoStreamFactory
from source_zoho_crm.types import (
    MODIFIED_TIME_SCHEMA_PROPERTY,
    FieldMeta,
    ModuleMeta,
    build_deals_fallback_schema,
)


CONFIG = {
    "client_id": "client-id",
    "client_secret": "client-secret",
    "refresh_token": "refresh-token",
    "dc_region": "EU",
    "edition": "Standard",
    "environment": "production",
    "start_datetime": "2020-01-01T00:00:00+00:00",
}

CONTACTS_FIELD = {
    "json_type": "string",
    "length": 255,
    "api_name": "Last_Name",
    "data_type": "text",
    "decimal_place": None,
    "system_mandatory": False,
    "display_label": "Last Name",
    "pick_list_values": [],
}

MODIFIED_TIME_FIELD = {
    "json_type": "string",
    "length": 120,
    "api_name": "Modified_Time",
    "data_type": "datetime",
    "decimal_place": None,
    "system_mandatory": False,
    "display_label": "Modified Time",
    "pick_list_values": [],
}


ConcreteIncrementalStream = type(
    "ConcreteIncrementalStream",
    (IncrementalZohoCrmStream,),
    {
        "url_base": "https://www.zohoapis.eu",
        "module": ModuleMeta(
            api_name="Contacts",
            module_name="Contacts",
            api_supported=True,
            fields=[FieldMeta.from_dict(CONTACTS_FIELD)],
        ),
    },
)


@pytest.fixture
def incremental_stream() -> IncrementalZohoCrmStream:
    return ConcreteIncrementalStream(authenticator=None, config=CONFIG)


def test_schema_modified_time_allows_null() -> None:
    module = ModuleMeta(
        api_name="Contacts",
        module_name="Contacts",
        api_supported=True,
        fields=[FieldMeta.from_dict(CONTACTS_FIELD), FieldMeta.from_dict(MODIFIED_TIME_FIELD)],
    )
    schema = asdict(module.schema)

    assert schema["properties"]["Modified_Time"] == MODIFIED_TIME_SCHEMA_PROPERTY
    assert "Modified_Time" in schema["required"]


def test_deals_fallback_schema_modified_time_allows_null() -> None:
    schema = asdict(build_deals_fallback_schema())

    assert schema["properties"]["Modified_Time"] == MODIFIED_TIME_SCHEMA_PROPERTY
    assert "Modified_Time" in schema["required"]


def test_record_without_modified_time_is_normalized_and_yielded(incremental_stream: IncrementalZohoCrmStream) -> None:
    records = [{"id": "1", "Last_Name": "Smith"}]

    with patch.object(ZohoCrmStream, "read_records", return_value=iter(records)):
        result = list(incremental_stream.read_records())

    assert len(result) == 1
    assert result[0]["Modified_Time"] is None


def test_record_with_modified_time_updates_state(incremental_stream: IncrementalZohoCrmStream) -> None:
    records = [{"id": "1", "Modified_Time": "2021-06-15T10:00:00+00:00"}]

    with patch.object(ZohoCrmStream, "read_records", return_value=iter(records)):
        result = list(incremental_stream.read_records())

    assert result == records
    assert incremental_stream.state["Modified_Time"] == "2021-06-15T10:00:00+00:00"


def test_record_with_null_modified_time_does_not_update_state(incremental_stream: IncrementalZohoCrmStream) -> None:
    records = [{"id": "1", "Modified_Time": None}]
    initial_state = dict(incremental_stream.state)

    with patch.object(ZohoCrmStream, "read_records", return_value=iter(records)):
        result = list(incremental_stream.read_records())

    assert result == records
    assert incremental_stream.state == initial_state


def test_state_uses_max_valid_modified_time_only(incremental_stream: IncrementalZohoCrmStream) -> None:
    records = [
        {"id": "1", "Modified_Time": "2021-06-15T10:00:00+00:00"},
        {"id": "2", "Last_Name": "No Timestamp"},
        {"id": "3", "Modified_Time": "2022-01-01T12:00:00+00:00"},
        {"id": "4", "Modified_Time": None},
    ]

    with patch.object(ZohoCrmStream, "read_records", return_value=iter(records)):
        result = list(incremental_stream.read_records())

    assert result[1]["Modified_Time"] is None
    assert incremental_stream.state["Modified_Time"] == "2022-01-01T12:00:00+00:00"


def test_deals_fallback_discovery_still_works() -> None:
    api = MagicMock()
    api.api_url = "https://www.zohoapis.eu"
    api.max_concurrent_requests = 5
    api.authenticator = MagicMock()
    api.modules_settings.return_value = [
        {"api_name": "Deals", "module_name": "Deals", "api_supported": True},
    ]
    api.module_settings.return_value = [{}]
    api.fields_settings.return_value = ([], True, 200)

    factory = ZohoStreamFactory.__new__(ZohoStreamFactory)
    factory._config = CONFIG
    factory.api = api

    streams = factory.produce()

    assert [stream.module.api_name for stream in streams] == ["Deals"]
    schema = streams[0].get_json_schema()
    assert schema is not None
    assert schema["properties"]["Modified_Time"] == MODIFIED_TIME_SCHEMA_PROPERTY


def test_field_meta_length_optional_still_works() -> None:
    field_without_length = {
        "json_type": "jsonobject",
        "api_name": "Owner",
        "data_type": "ownerlookup",
        "decimal_place": None,
        "system_mandatory": False,
        "display_label": "Owner",
        "pick_list_values": [],
    }

    field_meta = FieldMeta.from_dict(field_without_length)

    assert field_meta.length is None
    assert field_meta.schema is not None
