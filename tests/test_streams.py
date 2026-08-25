from unittest.mock import patch

import pytest

from source_zoho_crm.streams import IncrementalZohoCrmStream, ZohoCrmStream


ConcreteIncrementalZohoCrmStream = type(
    "ConcreteIncrementalZohoCrmStream",
    (IncrementalZohoCrmStream,),
    {"url_base": "https://www.zohoapis.com"},
)


@pytest.fixture
def incremental_stream() -> IncrementalZohoCrmStream:
    config = {"start_datetime": "2020-01-01T00:00:00+00:00"}
    return ConcreteIncrementalZohoCrmStream(authenticator=None, config=config)


def test_read_records_updates_state_when_modified_time_present(incremental_stream: IncrementalZohoCrmStream) -> None:
    records = [{"id": "1", "Modified_Time": "2021-06-15T10:00:00+00:00"}]

    with patch.object(ZohoCrmStream, "read_records", return_value=iter(records)):
        result = list(incremental_stream.read_records())

    assert result == records
    assert incremental_stream.state["Modified_Time"] == "2021-06-15T10:00:00+00:00"


def test_read_records_without_modified_time(incremental_stream: IncrementalZohoCrmStream) -> None:
    records = [{"id": "1", "Name": "Test Contact"}]
    initial_state = dict(incremental_stream.state)

    with patch.object(ZohoCrmStream, "read_records", return_value=iter(records)):
        result = list(incremental_stream.read_records())

    assert result == records
    assert incremental_stream.state == initial_state


def test_read_records_with_null_modified_time(incremental_stream: IncrementalZohoCrmStream) -> None:
    records = [{"id": "2", "Modified_Time": None, "Name": "Test Account"}]
    initial_state = dict(incremental_stream.state)

    with patch.object(ZohoCrmStream, "read_records", return_value=iter(records)):
        result = list(incremental_stream.read_records())

    assert result == records
    assert incremental_stream.state == initial_state
