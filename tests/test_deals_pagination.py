import logging
from unittest.mock import MagicMock, patch

import pytest
from airbyte_cdk.models import SyncMode

from source_zoho_crm.streams import DealsAvailabilityStrategy, IncrementalZohoCrmStream, ZohoCrmStream
from source_zoho_crm.types import FieldMeta, ModuleMeta, ZOHO_V8_MAX_PAGE_NUMBER, ZOHO_V8_RECORDS_PER_PAGE


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


def _response(page: int, more_records: bool, next_page_token: str | None = None, record_count: int = 200):
    response = MagicMock()
    response.status_code = 200
    info = {
        "page": page,
        "per_page": ZOHO_V8_RECORDS_PER_PAGE,
        "count": record_count,
        "more_records": more_records,
    }
    if next_page_token:
        info["next_page_token"] = next_page_token
    response.json.return_value = {
        "data": [{"id": str((page - 1) * ZOHO_V8_RECORDS_PER_PAGE + index)} for index in range(record_count)],
        "info": info,
    }
    return response


def _token_response(token_suffix: str, more_records: bool, next_page_token: str | None = None, record_count: int = 200):
    response = MagicMock()
    response.status_code = 200
    info = {
        "per_page": ZOHO_V8_RECORDS_PER_PAGE,
        "count": record_count,
        "more_records": more_records,
    }
    if next_page_token:
        info["next_page_token"] = next_page_token
    base_id = 2000 + int(token_suffix)
    response.json.return_value = {
        "data": [{"id": str(base_id + index)} for index in range(record_count)],
        "info": info,
    }
    return response


def _params_from_token(next_page_token):
    stream = _deals_stream()
    stream._active_field_batch = stream._field_request_batches()[0]
    return stream.request_params({}, next_page_token=next_page_token)


def test_1999_records_page_pagination_finishes_normally() -> None:
    stream = _deals_stream()
    stream._active_field_batch = stream._field_request_batches()[0]
    stream._active_field_batch_index = 0

    last_token = None
    for page in range(1, ZOHO_V8_MAX_PAGE_NUMBER + 1):
        record_count = 199 if page == 10 else ZOHO_V8_RECORDS_PER_PAGE
        more_records = page < 10
        response = _response(page, more_records, record_count=record_count)
        last_token = stream.next_page_token(response)

    assert last_token is None
    assert stream._pagination_uses_page_token is False


def test_page_10_with_more_records_switches_to_page_token() -> None:
    stream = _deals_stream()
    stream._active_field_batch = stream._field_request_batches()[0]
    stream._active_field_batch_index = 0

    response = _response(ZOHO_V8_MAX_PAGE_NUMBER, True, next_page_token="token-after-2000")
    next_token = stream.next_page_token(response)

    assert next_token == {"page_token": "token-after-2000"}
    assert "page" not in next_token


def test_v8_deals_never_generates_page_11() -> None:
    stream = _deals_stream()
    stream._active_field_batch = stream._field_request_batches()[0]
    stream._active_field_batch_index = 0

    generated_pages = []
    next_page_token = None

    for page in range(1, 15):
        if next_page_token is None and page == 1:
            params = stream.request_params({})
        else:
            params = stream.request_params({}, next_page_token=next_page_token)

        if "page" in params:
            generated_pages.append(params["page"])

        if page <= ZOHO_V8_MAX_PAGE_NUMBER:
            response = _response(page, True, next_page_token=f"token-{page}")
        else:
            break

        next_page_token = stream.next_page_token(response)

    assert 11 not in generated_pages
    assert max(generated_pages) == ZOHO_V8_MAX_PAGE_NUMBER


def test_page_token_request_has_no_page_parameter() -> None:
    params = _params_from_token({"page_token": "abc123"})

    assert params["page_token"] == "abc123"
    assert "page" not in params
    assert params["per_page"] == ZOHO_V8_RECORDS_PER_PAGE


def test_page_token_chain_token1_to_token2_to_end() -> None:
    stream = _deals_stream()
    stream._active_field_batch = stream._field_request_batches()[0]
    stream._pagination_uses_page_token = True

    first = _token_response("1", True, next_page_token="token-2")
    second = _token_response("2", True, next_page_token="token-3")
    third = _token_response("3", False)

    assert stream.next_page_token(first) == {"page_token": "token-2"}
    assert stream.next_page_token(second) == {"page_token": "token-3"}
    assert stream.next_page_token(third) is None


def test_two_field_batches_use_independent_pagination_tokens() -> None:
    stream = _deals_stream(field_count=60)
    batches = stream._field_request_batches()
    assert len(batches) == 2

    captured_params = []
    batch_one_fields = ",".join(batches[0])
    batch_two_fields = ",".join(batches[1])

    def fetch_side_effect(stream_slice=None, stream_state=None, next_page_token=None):
        params = stream.request_params(stream_state or {}, next_page_token=next_page_token)
        captured_params.append(dict(params))
        fields = params["fields"]

        if fields == batch_one_fields:
            if params.get("page_token") == "token-11":
                response = _token_response("11", False, record_count=1)
                response.json.return_value["data"] = [{"id": "1", "Pipeline": "P1", "Stage": "S"}]
            elif params.get("page") == 10:
                response = _response(10, True, next_page_token="token-11", record_count=1)
                response.json.return_value["data"] = [{"id": "1", "Pipeline": "P1"}]
            else:
                page = params.get("page", 1)
                response = _response(page, page < 10, record_count=1)
                response.json.return_value["data"] = [{"id": "1", "Pipeline": "P1"}]
        else:
            assert fields == batch_two_fields
            assert params.get("page_token") != "token-11"
            page = params.get("page", 1)
            response = _response(page, False, record_count=1)
            response.json.return_value["data"] = [{"id": "1", "Custom_Field_40": "x"}]

        return MagicMock(), response

    with patch.object(ZohoCrmStream, "_fetch_next_page", side_effect=fetch_side_effect):
        result = list(stream.read_records(sync_mode=SyncMode.full_refresh))

    batch_one_params = [params for params in captured_params if params["fields"] == batch_one_fields]
    batch_two_params = [params for params in captured_params if params["fields"] == batch_two_fields]

    assert batch_one_params
    assert batch_two_params
    assert batch_two_params[0]["page"] == 1
    assert "page_token" not in batch_two_params[0]
    assert any(params.get("page_token") == "token-11" for params in batch_one_params)
    assert all(params.get("page_token") != "token-11" for params in batch_two_params)
    assert len(result) == 1


def test_page_token_request_keeps_same_fields_as_generating_request() -> None:
    stream = _deals_stream(field_count=2)
    field_batch = stream._field_request_batches()[0]
    stream._active_field_batch = field_batch

    page_params = stream.request_params({})
    token_params = stream.request_params({}, next_page_token={"page_token": "tok-xyz"})

    assert page_params["fields"] == token_params["fields"]
    assert "Pipeline" in page_params["fields"].split(",")


def test_more_than_4000_records_returned_and_merged_by_id() -> None:
    stream = _deals_stream(field_count=0)
    captured_params = []

    def fetch_side_effect(stream_slice=None, stream_state=None, next_page_token=None):
        params = stream.request_params(stream_state or {}, next_page_token=next_page_token)
        captured_params.append(dict(params))

        if "page_token" in params:
            token_num = int(params["page_token"].replace("tok-", ""))
            more = token_num < 21
            next_tok = f"tok-{token_num + 1}" if more else None
            start_id = 2000 + (token_num - 11) * ZOHO_V8_RECORDS_PER_PAGE
            response = _token_response(str(token_num), more, next_page_token=next_tok, record_count=ZOHO_V8_RECORDS_PER_PAGE)
            response.json.return_value["data"] = [
                {"id": str(start_id + index), "Pipeline": "P"} for index in range(ZOHO_V8_RECORDS_PER_PAGE)
            ]
        else:
            page = params["page"]
            more = page <= 10
            next_tok = "tok-11" if page == 10 else None
            start_id = (page - 1) * ZOHO_V8_RECORDS_PER_PAGE
            response = _response(
                page,
                more and (page < 10 or page == 10),
                next_page_token=next_tok,
                record_count=ZOHO_V8_RECORDS_PER_PAGE,
            )
            response.json.return_value["data"] = [
                {"id": str(start_id + index), "Pipeline": "P"} for index in range(ZOHO_V8_RECORDS_PER_PAGE)
            ]

        return MagicMock(), response

    with patch.object(ZohoCrmStream, "_fetch_next_page", side_effect=fetch_side_effect):
        result = list(stream.read_records(sync_mode=SyncMode.full_refresh))

    page_numbers = [params["page"] for params in captured_params if "page" in params]

    assert len(result) >= 4000
    assert 11 not in page_numbers
    assert max(page_numbers) == 10
    assert all(record.get("Pipeline") == "P" for record in result[:5])


def test_pipeline_populated_after_multi_batch_pagination() -> None:
    stream = _deals_stream(field_count=60)

    batch_one = [{"id": "1", "Stage": "Open", "Pipeline": "Діагностика"}]
    batch_two = [{"id": "1", "Custom_Field_40": "extra"}]

    with patch(
        "source_zoho_crm.streams.HttpStream.read_records",
        side_effect=[iter(batch_one), iter(batch_two)],
    ):
        result = list(stream.read_records(sync_mode=SyncMode.full_refresh))

    assert len(result) == 1
    assert result[0]["Pipeline"] == "Діагностика"


def test_availability_check_does_not_read_all_records() -> None:
    stream = _deals_stream(field_count=60)
    strategy = DealsAvailabilityStrategy()

    with patch.object(ZohoCrmStream, "read_records") as mock_read_records:
        with patch.object(ZohoCrmStream, "_fetch_next_page") as mock_fetch:
            mock_fetch.return_value = (MagicMock(), _response(1, False, record_count=1))
            is_available, reason = strategy.check_availability(stream, logging.getLogger("test"))

    assert is_available is True
    assert reason is None
    mock_read_records.assert_not_called()
    mock_fetch.assert_called_once()


def test_regression_fix7_pipeline_still_requested() -> None:
    stream = _deals_stream(field_count=2)
    stream._active_field_batch = stream._field_request_batches()[0]
    params = stream.request_params({})

    assert stream.path() == "/crm/v8/Deals"
    assert "Pipeline" in params["fields"].split(",")
    assert params["per_page"] == ZOHO_V8_RECORDS_PER_PAGE
