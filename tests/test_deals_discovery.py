from unittest.mock import MagicMock, patch

import pytest

from source_zoho_crm.streams import IncrementalZohoCrmStream, ZohoCrmStream, ZohoStreamFactory
from source_zoho_crm.types import FieldMeta, ModuleMeta


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


def _build_factory_with_api(api: MagicMock) -> ZohoStreamFactory:
    api.supports_deleted_records.return_value = False
    factory = ZohoStreamFactory.__new__(ZohoStreamFactory)
    factory._config = CONFIG
    factory.api = api
    return factory


def test_deals_in_catalog_when_fields_metadata_204() -> None:
    api = MagicMock()
    api.api_url = "https://www.zohoapis.eu"
    api.max_concurrent_requests = 5
    api.authenticator = MagicMock()
    api.modules_settings.return_value = [
        {"api_name": "Deals", "module_name": "Deals", "api_supported": True},
        {"api_name": "Contacts", "module_name": "Contacts", "api_supported": True},
    ]
    api.module_settings.return_value = [{}]

    def fields_side_effect(module_name: str):
        if module_name == "Deals":
            return [], True, 204
        return [CONTACTS_FIELD], False, 200

    api.fields_settings.side_effect = fields_side_effect

    streams = _build_factory_with_api(api).produce()
    stream_names = [stream.module.api_name for stream in streams]

    assert "Deals" in stream_names
    assert "Contacts" in stream_names

    deals_stream = next(stream for stream in streams if stream.module.api_name == "Deals")
    schema = deals_stream.get_json_schema()

    assert schema is not None
    assert schema["description"] == "Deals"
    assert "Stage" in schema["properties"]
    assert "Pipeline" in schema["properties"]
    assert "Owner" in schema["properties"]
    assert deals_stream.path() == "/crm/v8/Deals"
    batches = deals_stream._field_request_batches()
    assert batches
    assert any("Pipeline" in batch for batch in batches)
    deals_stream._active_field_batch = batches[0]
    params = deals_stream.request_params({})
    assert "Pipeline" in params["fields"].split(",")


def test_deals_record_read_with_critical_fields() -> None:
    module = ModuleMeta(
        api_name="Deals",
        module_name="Deals",
        api_supported=True,
        fields_metadata_unavailable=True,
    )
    deals_stream_cls = type(
        "IncrementalDealsZohoCRMStream",
        (IncrementalZohoCrmStream,),
        {"url_base": "https://www.zohoapis.eu", "module": module},
    )
    stream = deals_stream_cls(authenticator=None, config=CONFIG)
    record = {
        "id": "123456789",
        "Deal_Name": "Enterprise Deal",
        "Stage": "Qualification",
        "Pipeline": "Standard Pipeline",
        "Owner": {"id": "1", "name": "John Doe", "email": "john@example.com"},
        "Modified_Time": "2024-01-15T10:00:00+00:00",
        "Created_Time": "2024-01-01T08:00:00+00:00",
    }

    with patch.object(ZohoCrmStream, "read_records", return_value=iter([record])):
        result = list(stream.read_records())

    assert result == [record]
    assert stream.state["Modified_Time"] == "2024-01-15T10:00:00+00:00"


def test_deals_record_without_modified_time_still_yields() -> None:
    module = ModuleMeta(
        api_name="Deals",
        module_name="Deals",
        api_supported=True,
        fields_metadata_unavailable=True,
    )
    deals_stream_cls = type(
        "IncrementalDealsZohoCRMStream",
        (IncrementalZohoCrmStream,),
        {"url_base": "https://www.zohoapis.eu", "module": module},
    )
    stream = deals_stream_cls(authenticator=None, config=CONFIG)
    record = {
        "id": "987654321",
        "Deal_Name": "Deal Without Modified Time",
        "Stage": "New",
        "Pipeline": "Sales Pipeline",
    }
    initial_state = dict(stream.state)

    with patch.object(ZohoCrmStream, "read_records", return_value=iter([record])):
        result = list(stream.read_records())

    assert result == [{**record, "Modified_Time": None}]
    assert stream.state == initial_state


def test_existing_streams_not_broken_when_deals_uses_fallback() -> None:
    contacts_module = ModuleMeta(
        api_name="Contacts",
        module_name="Contacts",
        api_supported=True,
        fields=[FieldMeta.from_dict(CONTACTS_FIELD)],
    )
    contacts_stream_cls = type(
        "IncrementalContactsZohoCRMStream",
        (IncrementalZohoCrmStream,),
        {"url_base": "https://www.zohoapis.eu", "module": contacts_module},
    )
    contacts_stream = contacts_stream_cls(authenticator=None, config=CONFIG)
    contacts_schema = contacts_stream.get_json_schema()

    assert contacts_schema is not None
    assert "Last_Name" in contacts_schema["properties"]
    assert contacts_stream.path() == "/crm/v2/Contacts"

    invoices_module = ModuleMeta(
        api_name="Invoices",
        module_name="Invoices",
        api_supported=True,
        fields_metadata_unavailable=True,
    )
    invoices_stream_cls = type(
        "IncrementalInvoicesZohoCRMStream",
        (IncrementalZohoCrmStream,),
        {"url_base": "https://www.zohoapis.eu", "module": invoices_module},
    )
    invoices_stream = invoices_stream_cls(authenticator=None, config=CONFIG)

    assert invoices_stream.get_json_schema() is None


def test_deals_in_catalog_when_fields_metadata_200_empty_array() -> None:
    api = MagicMock()
    api.api_url = "https://www.zohoapis.eu"
    api.max_concurrent_requests = 5
    api.authenticator = MagicMock()
    api.modules_settings.return_value = [
        {"api_name": "Deals", "module_name": "Deals", "api_supported": True},
        {"api_name": "Contacts", "module_name": "Contacts", "api_supported": True},
    ]
    api.module_settings.return_value = [{}]

    def fields_side_effect(module_name: str):
        if module_name == "Deals":
            return [], True, 200
        return [CONTACTS_FIELD], False, 200

    api.fields_settings.side_effect = fields_side_effect

    streams = _build_factory_with_api(api).produce()
    stream_names = [stream.module.api_name for stream in streams]

    assert "Deals" in stream_names
    deals_stream = next(stream for stream in streams if stream.module.api_name == "Deals")
    assert deals_stream.module.fields_metadata_unavailable is True
    assert deals_stream.get_json_schema() is not None


def test_deals_in_catalog_when_module_settings_empty_but_fields_204() -> None:
    api = MagicMock()
    api.api_url = "https://www.zohoapis.eu"
    api.max_concurrent_requests = 5
    api.authenticator = MagicMock()
    api.modules_settings.return_value = [
        {"api_name": "Deals", "module_name": "Deals", "api_supported": True},
    ]
    api.module_settings.return_value = []

    def fields_side_effect(module_name: str):
        return [], True, 204

    api.fields_settings.side_effect = fields_side_effect

    streams = _build_factory_with_api(api).produce()

    assert [stream.module.api_name for stream in streams] == ["Deals"]


def test_deals_fallback_via_api_fields_settings_200_empty() -> None:
    from unittest.mock import MagicMock, patch

    from source_zoho_crm.api import ZohoAPI

    api = ZohoAPI(CONFIG)
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"fields": []}
    response.raise_for_status = MagicMock()

    with patch.object(api.authenticator, "get_auth_header", return_value={"Authorization": "Bearer test"}):
        with patch("source_zoho_crm.api.requests.get", return_value=response):
            fields, metadata_unavailable, http_status = api.fields_settings("Deals")

    assert http_status == 200
    assert fields == []
    assert metadata_unavailable is True

