import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from source_zoho_crm.deleted_streams import EMPTY_BODY_STATUSES, DeletedZohoCrmStream
from source_zoho_crm.streams import ZohoStreamFactory
from source_zoho_crm.types import (
    DELETED_RECORDS_SUPPORTED_API_NAMES,
    ModuleMeta,
    build_deleted_record_schema,
    is_deleted_stream_candidate,
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


def _deleted_stream(module_api_name: str = "Deals") -> DeletedZohoCrmStream:
    module = ModuleMeta(api_name=module_api_name, module_name=module_api_name, api_supported=True)
    stream_cls = type(
        f"IncrementalDeleted{module_api_name}ZohoCRMStream",
        (DeletedZohoCrmStream,),
        {"url_base": "https://www.zohoapis.eu", "module": module},
    )
    return stream_cls(authenticator=None, config=CONFIG)


def _mock_response(status_code: int, payload: dict | None = None) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    if payload is None:
        response.json.side_effect = ValueError("no body")
    else:
        response.json.return_value = payload
    return response


def test_deleted_stream_pagination_two_pages() -> None:
    stream = _deleted_stream("Deals")
    page_one = _mock_response(
        200,
        {
            "data": [{"id": "1", "deleted_time": "2024-01-01T10:00:00+00:00", "type": "recycle"}],
            "info": {"page": 1, "more_records": True},
        },
    )
    page_two = _mock_response(
        200,
        {
            "data": [{"id": "2", "deleted_time": "2024-01-02T10:00:00+00:00", "type": "permanent"}],
            "info": {"page": 2, "more_records": False},
        },
    )

    assert stream.next_page_token(page_one) == {"page": 2}
    assert stream.next_page_token(page_two) is None

    records = list(stream.parse_response(page_one)) + list(stream.parse_response(page_two))
    assert len(records) == 2
    assert records[0]["module_api_name"] == "Deals"
    assert records[1]["type"] == "permanent"


def test_deleted_stream_204_is_successful_empty_response() -> None:
    stream = _deleted_stream("Contacts")
    response = _mock_response(204)

    assert stream.next_page_token(response) is None
    assert list(stream.parse_response(response)) == []


def test_deleted_stream_state_uses_max_deleted_time() -> None:
    stream = _deleted_stream("Deals")
    records = [
        {"id": "1", "deleted_time": "2024-01-01T10:00:00+00:00", "type": "recycle", "module_api_name": "Deals"},
        {"id": "2", "deleted_time": "2024-06-01T12:00:00+00:00", "type": "permanent", "module_api_name": "Deals"},
    ]

    with patch.object(DeletedZohoCrmStream, "read_records", wraps=stream.read_records):
        with patch("source_zoho_crm.deleted_streams.HttpStream.read_records", return_value=iter(records)):
            result = list(stream.read_records())

    assert len(result) == 2
    assert stream.state["deleted_time"] == "2024-06-01T12:00:00+00:00"


def test_deleted_stream_request_headers_use_if_modified_since() -> None:
    stream = _deleted_stream("Invoices")
    stream.state = {"deleted_time": "2024-03-15T08:30:00+00:00"}

    headers = stream.request_headers(stream.state)

    assert "If-Modified-Since" in headers
    assert headers["If-Modified-Since"] == "2024-03-15T08:30:01+00:00"


def test_recycle_and_permanent_for_same_id_both_yield() -> None:
    stream = _deleted_stream("Deals")
    records = [
        {"id": "same-id", "deleted_time": "2024-01-01T10:00:00+00:00", "type": "recycle", "module_api_name": "Deals"},
        {"id": "same-id", "deleted_time": "2024-02-01T10:00:00+00:00", "type": "permanent", "module_api_name": "Deals"},
    ]

    with patch("source_zoho_crm.deleted_streams.HttpStream.read_records", return_value=iter(records)):
        result = list(stream.read_records())

    assert len(result) == 2
    assert result[0]["type"] == "recycle"
    assert result[1]["type"] == "permanent"


def test_unsupported_module_does_not_break_catalog() -> None:
    api = MagicMock()
    api.api_url = "https://www.zohoapis.eu"
    api.max_concurrent_requests = 5
    api.authenticator = MagicMock()
    api.modules_settings.return_value = [
        {"api_name": "Deals", "module_name": "Deals", "api_supported": True},
        {"api_name": "Notes", "module_name": "Notes", "api_supported": True},
    ]
    api.module_settings.return_value = [{}]
    api.fields_settings.return_value = ([CONTACTS_FIELD], False, 200)

    def supports_deleted(module_api_name: str) -> bool:
        return module_api_name == "Deals"

    api.supports_deleted_records.side_effect = supports_deleted

    factory = ZohoStreamFactory.__new__(ZohoStreamFactory)
    factory._config = CONFIG
    factory.api = api

    streams = factory.produce()
    stream_names = [stream.name for stream in streams]

    assert any("deals" in name and "deleted" not in name for name in stream_names)
    assert any("deleted" in name and "deals" in name for name in stream_names)
    assert not any("notes" in name and "deleted" in name for name in stream_names)


def test_supported_available_module_adds_deleted_stream() -> None:
    api = MagicMock()
    api.api_url = "https://www.zohoapis.eu"
    api.max_concurrent_requests = 5
    api.authenticator = MagicMock()
    api.modules_settings.return_value = [
        {"api_name": "Contacts", "module_name": "Contacts", "api_supported": True},
    ]
    api.module_settings.return_value = [{}]
    api.fields_settings.return_value = ([CONTACTS_FIELD], False, 200)
    api.supports_deleted_records.return_value = True

    factory = ZohoStreamFactory.__new__(ZohoStreamFactory)
    factory._config = CONFIG
    factory.api = api

    streams = factory.produce()
    deleted_streams = [stream for stream in streams if "deleted" in stream.name]

    assert len(deleted_streams) == 1
    assert deleted_streams[0].module.api_name == "Contacts"
    assert deleted_streams[0].name == "incremental_deleted_contacts_zoho_crm_stream"


def test_module_absent_from_zoho_does_not_create_deleted_stream() -> None:
    assert "Price_Books" in DELETED_RECORDS_SUPPORTED_API_NAMES
    assert is_deleted_stream_candidate("Price_Books") is True

    api = MagicMock()
    api.api_url = "https://www.zohoapis.eu"
    api.max_concurrent_requests = 5
    api.authenticator = MagicMock()
    api.modules_settings.return_value = [
        {"api_name": "Contacts", "module_name": "Contacts", "api_supported": True},
    ]
    api.module_settings.return_value = [{}]
    api.fields_settings.return_value = ([CONTACTS_FIELD], False, 200)
    api.supports_deleted_records.return_value = True

    factory = ZohoStreamFactory.__new__(ZohoStreamFactory)
    factory._config = CONFIG
    factory.api = api

    streams = factory.produce()
    deleted_api_names = [stream.module.api_name for stream in streams if "deleted" in stream.name]

    assert deleted_api_names == ["Contacts"]
    assert "Price_Books" not in deleted_api_names


def test_deleted_record_schema_shape() -> None:
    schema = build_deleted_record_schema("Deals")

    assert "deleted_time" in schema.properties
    assert schema.properties["deleted_time"]["type"] == ["null", "string"]
    assert schema.properties["module_api_name"]["type"] == "string"


def test_api_invalid_module_is_not_supported() -> None:
    from source_zoho_crm.api import ZohoAPI

    api = ZohoAPI(CONFIG)
    response = MagicMock()
    response.status_code = 400
    response.json.return_value = {"code": "INVALID_MODULE", "message": "the module name given is invalid"}

    with patch.object(api.authenticator, "get_auth_header", return_value={"Authorization": "Bearer test"}):
        with patch("source_zoho_crm.api.requests.get", return_value=response):
            assert api.supports_deleted_records("Notes") is False


def test_api_auth_errors_are_not_swallowed() -> None:
    from source_zoho_crm.api import ZohoAPI

    api = ZohoAPI(CONFIG)
    response = MagicMock()
    response.status_code = 403
    response.raise_for_status.side_effect = requests.HTTPError(response=response)

    with patch.object(api.authenticator, "get_auth_header", return_value={"Authorization": "Bearer test"}):
        with patch("source_zoho_crm.api.requests.get", return_value=response):
            with pytest.raises(requests.HTTPError):
                api.supports_deleted_records("Deals")
