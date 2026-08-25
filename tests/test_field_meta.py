from dataclasses import asdict
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

FIELD_WITH_LENGTH = {
    "json_type": "string",
    "length": 255,
    "api_name": "Last_Name",
    "data_type": "text",
    "decimal_place": None,
    "system_mandatory": False,
    "display_label": "Last Name",
    "pick_list_values": [],
}

FIELD_WITHOUT_LENGTH = {
    "json_type": "jsonobject",
    "api_name": "Owner",
    "data_type": "ownerlookup",
    "decimal_place": None,
    "system_mandatory": False,
    "display_label": "Owner",
    "pick_list_values": [],
}

STRING_FIELD_WITHOUT_LENGTH = {
    "json_type": "string",
    "api_name": "Custom_Text",
    "data_type": "text",
    "decimal_place": None,
    "system_mandatory": False,
    "display_label": "Custom Text",
    "pick_list_values": [],
}


def test_field_meta_from_dict_without_length() -> None:
    field_meta = FieldMeta.from_dict(FIELD_WITHOUT_LENGTH)

    assert field_meta.length is None
    assert field_meta.api_name == "Owner"
    assert field_meta.data_type == "ownerlookup"


def test_string_field_schema_without_length_omits_max_length() -> None:
    field_meta = FieldMeta.from_dict(STRING_FIELD_WITHOUT_LENGTH)
    schema = field_meta.schema

    assert schema["type"] == ["null", "string"]
    assert "maxLength" not in schema


def test_string_field_schema_with_length_preserves_max_length() -> None:
    field_meta = FieldMeta.from_dict(FIELD_WITH_LENGTH)
    schema = field_meta.schema

    assert schema["maxLength"] == 255


def test_module_schema_with_mixed_length_fields() -> None:
    module = ModuleMeta(
        api_name="Contacts",
        module_name="Contacts",
        api_supported=True,
        fields=[
            FieldMeta.from_dict(FIELD_WITH_LENGTH),
            FieldMeta.from_dict(FIELD_WITHOUT_LENGTH),
        ],
    )

    schema = asdict(module.schema)

    assert "Last_Name" in schema["properties"]
    assert "Owner" in schema["properties"]
    assert "maxLength" in schema["properties"]["Last_Name"]
    assert "maxLength" not in schema["properties"]["Owner"]


def _build_factory_with_api(api: MagicMock) -> ZohoStreamFactory:
    factory = ZohoStreamFactory.__new__(ZohoStreamFactory)
    factory._config = CONFIG
    factory.api = api
    return factory


def test_factory_produce_with_field_missing_length() -> None:
    api = MagicMock()
    api.api_url = "https://www.zohoapis.eu"
    api.max_concurrent_requests = 5
    api.authenticator = MagicMock()
    api.modules_settings.return_value = [
        {"api_name": "Contacts", "module_name": "Contacts", "api_supported": True},
    ]
    api.module_settings.return_value = [{}]
    api.fields_settings.return_value = (
        [FIELD_WITH_LENGTH, FIELD_WITHOUT_LENGTH],
        False,
        200,
    )

    streams = _build_factory_with_api(api).produce()

    assert len(streams) == 1
    assert streams[0].module.api_name == "Contacts"
    assert streams[0].get_json_schema() is not None


def test_approvals_204_excluded_without_fallback() -> None:
    api = MagicMock()
    api.api_url = "https://www.zohoapis.eu"
    api.max_concurrent_requests = 5
    api.authenticator = MagicMock()
    api.modules_settings.return_value = [
        {"api_name": "Approvals", "module_name": "Approvals", "api_supported": True},
    ]
    api.module_settings.return_value = [{}]
    api.fields_settings.return_value = ([], True, 204)

    streams = _build_factory_with_api(api).produce()

    assert streams == []


def test_deals_fallback_200_empty_still_works() -> None:
    api = MagicMock()
    api.api_url = "https://www.zohoapis.eu"
    api.max_concurrent_requests = 5
    api.authenticator = MagicMock()
    api.modules_settings.return_value = [
        {"api_name": "Deals", "module_name": "Deals", "api_supported": True},
    ]
    api.module_settings.return_value = [{}]
    api.fields_settings.return_value = ([], True, 200)

    streams = _build_factory_with_api(api).produce()

    assert [stream.module.api_name for stream in streams] == ["Deals"]


def test_deals_fallback_204_still_works() -> None:
    api = MagicMock()
    api.api_url = "https://www.zohoapis.eu"
    api.max_concurrent_requests = 5
    api.authenticator = MagicMock()
    api.modules_settings.return_value = [
        {"api_name": "Deals", "module_name": "Deals", "api_supported": True},
    ]
    api.module_settings.return_value = [{}]
    api.fields_settings.return_value = ([], True, 204)

    streams = _build_factory_with_api(api).produce()

    assert [stream.module.api_name for stream in streams] == ["Deals"]


def test_modified_time_fix_still_yields_without_modified_time() -> None:
    module = ModuleMeta(
        api_name="Contacts",
        module_name="Contacts",
        api_supported=True,
        fields=[FieldMeta.from_dict(FIELD_WITHOUT_LENGTH)],
    )
    stream_cls = type(
        "IncrementalContactsZohoCRMStream",
        (IncrementalZohoCrmStream,),
        {"url_base": "https://www.zohoapis.eu", "module": module},
    )
    stream = stream_cls(authenticator=None, config=CONFIG)
    record = {"id": "1", "Owner": {"id": "2", "name": "Jane"}}
    initial_state = dict(stream.state)

    with patch.object(ZohoCrmStream, "read_records", return_value=iter([record])):
        result = list(stream.read_records())

    assert result == [record]
    assert stream.state == initial_state
