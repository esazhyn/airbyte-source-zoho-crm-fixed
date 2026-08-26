#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import datetime
import logging
from dataclasses import asdict
from http import HTTPStatus
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional

import requests

from airbyte_cdk.sources.streams.http import HttpStream

from .types import ModuleMeta, build_deleted_record_schema


logger = logging.getLogger(__name__)

EMPTY_BODY_STATUSES = (HTTPStatus.NO_CONTENT, HTTPStatus.NOT_MODIFIED)
DELETED_RECORDS_PAGE_SIZE = 200


class DeletedZohoCrmStream(HttpStream):
    cursor_field = "deleted_time"
    primary_key = "id"
    module: ModuleMeta = None

    def __init__(self, authenticator: "requests.auth.AuthBase" = None, config: Mapping[str, Any] = None):
        super().__init__(authenticator)
        self._config = config or {}
        self._state: Dict[str, Any] = {}
        self._start_datetime = self._config.get("start_datetime") or "1970-01-01T00:00:00+00:00"

    @property
    def state(self) -> Mapping[str, Any]:
        if not self._state:
            self._state = {self.cursor_field: self._start_datetime}
        return self._state

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._state = dict(value)

    def path(self, *args, **kwargs) -> str:
        return f"/crm/v8/{self.module.api_name}/deleted"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params: MutableMapping[str, Any] = {"type": "all", "per_page": DELETED_RECORDS_PAGE_SIZE}
        if next_page_token:
            params.update(next_page_token)
        else:
            params["page"] = 1
        return params

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        last_deleted = stream_state.get(self.cursor_field, self._start_datetime)
        last_deleted_dt = datetime.datetime.fromisoformat(last_deleted)
        last_deleted_dt += datetime.timedelta(seconds=1)
        return {"If-Modified-Since": last_deleted_dt.isoformat("T", "seconds")}

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if response.status_code in EMPTY_BODY_STATUSES:
            return None
        pagination = response.json()["info"]
        if not pagination["more_records"]:
            return None
        return {"page": pagination["page"] + 1}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping[str, Any]]:
        if response.status_code in EMPTY_BODY_STATUSES:
            return
        for record in response.json().get("data", []):
            enriched = dict(record)
            enriched["module_api_name"] = self.module.api_name
            yield enriched

    def get_json_schema(self) -> Optional[Dict[Any, Any]]:
        return asdict(build_deleted_record_schema(self.module.api_name))

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        records_read = 0
        for record in super().read_records(*args, **kwargs):
            if self.cursor_field not in record:
                record = dict(record)
                record[self.cursor_field] = None
            deleted_time = record.get(self.cursor_field)
            if deleted_time:
                current_cursor_value = datetime.datetime.fromisoformat(self.state[self.cursor_field])
                latest_cursor_value = datetime.datetime.fromisoformat(deleted_time)
                new_cursor_value = max(latest_cursor_value, current_cursor_value)
                self.state = {self.cursor_field: new_cursor_value.isoformat("T", "seconds")}
            records_read += 1
            yield record
        self.logger.info(
            "Deleted records read=%s state=%s module_api_name=%s stream=%s",
            records_read,
            self.state,
            self.module.api_name,
            self.name,
        )
