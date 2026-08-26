#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import concurrent.futures
import datetime
import logging
import math
from abc import ABC
from dataclasses import asdict
from http import HTTPStatus
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional

import requests
from requests.exceptions import HTTPError

from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.availability_strategy import HttpAvailabilityStrategy

from .api import ZohoAPI
from .deleted_streams import DeletedZohoCrmStream
from .exceptions import IncompleteMetaDataException, UnknownDataTypeException
from .types import (
    DEALS_MANDATORY_RECORD_FIELDS,
    FieldMeta,
    ModuleMeta,
    ZohoPickListItem,
    ZOHO_RECORDS_MAX_FIELDS_PER_REQUEST,
    ZOHO_V8_MAX_PAGE_NUMBER,
    ZOHO_V8_RECORDS_PER_PAGE,
    build_record_field_batches,
    collect_module_record_field_names,
    is_deals_module,
    is_deleted_stream_candidate,
)


# 204 and 304 status codes are valid successful responses,
# but `.json()` will fail because the response body is empty
EMPTY_BODY_STATUSES = (HTTPStatus.NO_CONTENT, HTTPStatus.NOT_MODIFIED)

logger = logging.getLogger(__name__)


class DealsAvailabilityStrategy(HttpAvailabilityStrategy):
    """Lightweight availability probe for Deals — one page=1 request, no full sync."""

    def check_availability(self, stream, logger: logging.Logger, source=None):
        if not isinstance(stream, ZohoCrmStream) or not is_deals_module(stream.module.api_name, stream.module.module_name):
            return super().check_availability(stream, logger, source)

        try:
            stream._reset_pagination_state()
            stream._active_field_batch = ["id"]
            stream_state = stream.state if hasattr(stream, "state") else {}
            stream._fetch_next_page(stream_state=stream_state)
            return True, None
        except HTTPError as error:
            is_available, reason = self.handle_http_error(stream, logger, source, error)
            if not is_available:
                reason = f"Unable to read {stream.name} stream. {reason}"
            return is_available, reason
        finally:
            stream._active_field_batch = None
            stream._reset_pagination_state()


class ZohoCrmStream(HttpStream, ABC):
    primary_key: str = "id"
    module: ModuleMeta = None
    _active_field_batch: Optional[List[str]] = None
    _active_field_batch_index: Optional[int] = None
    _pagination_uses_page_token: bool = False

    def _collect_record_field_names(self) -> List[str]:
        return collect_module_record_field_names(self.module)

    def _requires_explicit_fields(self) -> bool:
        if is_deals_module(self.module.api_name, self.module.module_name):
            return True
        return len(self._collect_record_field_names()) > ZOHO_RECORDS_MAX_FIELDS_PER_REQUEST

    def _field_request_batches(self) -> List[List[str]]:
        if not self._requires_explicit_fields():
            return []
        mandatory = (
            DEALS_MANDATORY_RECORD_FIELDS
            if is_deals_module(self.module.api_name, self.module.module_name)
            else ("id", "Modified_Time")
        )
        return build_record_field_batches(self._collect_record_field_names(), mandatory)

    def _records_api_path(self) -> str:
        if self._requires_explicit_fields():
            return f"/crm/v8/{self.module.api_name}"
        return f"/crm/v2/{self.module.api_name}"

    def _reset_pagination_state(self) -> None:
        self._pagination_uses_page_token = False

    @property
    def availability_strategy(self):
        if is_deals_module(self.module.api_name, self.module.module_name):
            return DealsAvailabilityStrategy()
        return HttpAvailabilityStrategy()

    def _log_deals_fields_batch(self, batch_index: int, field_batch: List[str]) -> None:
        if is_deals_module(self.module.api_name, self.module.module_name):
            logger.info(
                "Deals request fields batch %s contains Pipeline=%s",
                batch_index + 1,
                "Pipeline" in field_batch,
            )

    def _log_deals_pagination(
        self,
        *,
        mode: str,
        page: Optional[int] = None,
        more_records: Optional[bool] = None,
        next_page_token_present: Optional[bool] = None,
    ) -> None:
        if not is_deals_module(self.module.api_name, self.module.module_name):
            return
        batch_number = (self._active_field_batch_index or 0) + 1
        logger.info(
            "Deals pagination: field_batch=%s mode=%s page=%s more_records=%s next_page_token_present=%s",
            batch_number,
            mode,
            page,
            more_records,
            next_page_token_present,
        )

    def _log_deals_pagination_switch_to_page_token(self) -> None:
        if not is_deals_module(self.module.api_name, self.module.module_name):
            return
        batch_number = (self._active_field_batch_index or 0) + 1
        logger.info("Deals pagination switching to page_token: field_batch=%s", batch_number)

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        batches = self._field_request_batches()
        if not batches:
            yield from super().read_records(*args, **kwargs)
            return

        if len(batches) == 1:
            self._active_field_batch_index = 0
            self._active_field_batch = batches[0]
            self._reset_pagination_state()
            self._log_deals_fields_batch(0, batches[0])
            try:
                yield from super().read_records(*args, **kwargs)
            finally:
                self._active_field_batch = None
                self._active_field_batch_index = None
                self._reset_pagination_state()
            return

        merged: Dict[str, MutableMapping[str, Any]] = {}
        for batch_index, field_batch in enumerate(batches):
            if is_deals_module(self.module.api_name, self.module.module_name):
                logger.info(
                    "Deals starting field batch %s/%s; pagination reset",
                    batch_index + 1,
                    len(batches),
                )
            self._active_field_batch_index = batch_index
            self._active_field_batch = field_batch
            self._reset_pagination_state()
            self._log_deals_fields_batch(batch_index, field_batch)
            try:
                for record in super().read_records(*args, **kwargs):
                    record_id = record.get("id")
                    if record_id is None:
                        yield record
                        continue
                    if record_id not in merged:
                        merged[record_id] = dict(record)
                    else:
                        merged[record_id].update(record)
            finally:
                self._active_field_batch = None
                self._active_field_batch_index = None
                self._reset_pagination_state()
        for record in merged.values():
            yield record

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if response.status_code in EMPTY_BODY_STATUSES:
            return None
        pagination = response.json()["info"]
        if not pagination["more_records"]:
            return None

        if not self._requires_explicit_fields():
            return {"page": pagination["page"] + 1}

        current_page = pagination.get("page")
        next_token = pagination.get("next_page_token")
        token_present = bool(next_token)

        if self._pagination_uses_page_token:
            self._log_deals_pagination(
                mode="page_token",
                page=current_page,
                more_records=True,
                next_page_token_present=token_present,
            )
            if next_token:
                return {"page_token": next_token}
            return None

        self._log_deals_pagination(
            mode="page",
            page=current_page,
            more_records=True,
            next_page_token_present=token_present,
        )

        if current_page is not None and current_page < ZOHO_V8_MAX_PAGE_NUMBER:
            return {"page": current_page + 1}

        if next_token:
            self._log_deals_pagination_switch_to_page_token()
            return {"page_token": next_token}

        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = dict(next_page_token or {})

        if self._requires_explicit_fields():
            params["per_page"] = ZOHO_V8_RECORDS_PER_PAGE
            if self._active_field_batch:
                params["fields"] = ",".join(self._active_field_batch)

            if "page_token" in params:
                params.pop("page", None)
                self._pagination_uses_page_token = True
            elif "page" in params:
                self._pagination_uses_page_token = False
            elif not self._pagination_uses_page_token:
                params["page"] = 1
        elif "page" not in params:
            params["page"] = 1

        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = [] if response.status_code in EMPTY_BODY_STATUSES else response.json()["data"]
        yield from data

    def path(self, *args, **kwargs) -> str:
        return self._records_api_path()

    def get_json_schema(self) -> Optional[Dict[Any, Any]]:
        try:
            schema = asdict(self.module.schema)
            if is_deals_module(self.module.api_name, self.module.module_name) and self.module.fields_metadata_unavailable:
                self.logger.warning(
                    "Fields metadata unavailable for module api_name=%s; enabling fallback Deals stream",
                    self.module.api_name,
                )
                self.logger.info("Deals added to catalog using fallback schema")
            return schema
        except IncompleteMetaDataException:
            # to build a schema for a stream, a sequence of requests is made:
            # one `/settings/modules` which introduces a list of modules,
            # one `/settings/modules/{module_name}` per module and
            # one `/settings/fields?module={module_name}` per module.
            # Any of former two can result in 204 and empty body what blocks us
            # from generating stream schema and, therefore, a stream.
            self.logger.warning(
                f"Could not retrieve fields Metadata for module {self.module.api_name}. "
                f"This stream will not be available for syncs."
            )
            return None
        except UnknownDataTypeException as exc:
            self.logger.warning(f"Unknown data type in module {self.module.api_name}, skipping. Details: {exc}")
            raise


class IncrementalZohoCrmStream(ZohoCrmStream):
    cursor_field = "Modified_Time"

    def __init__(self, authenticator: "requests.auth.AuthBase" = None, config: Mapping[str, Any] = None):
        super().__init__(authenticator)
        self._config = config
        self._state = {}
        self._start_datetime = self._config.get("start_datetime") or "1970-01-01T00:00:00+00:00"

    @property
    def state(self) -> Mapping[str, Any]:
        if not self._state:
            self._state = {self.cursor_field: self._start_datetime}
        return self._state

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._state = value

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            if self.cursor_field not in record:
                record = dict(record)
                record[self.cursor_field] = None
            modified_time = record.get(self.cursor_field)
            if modified_time:
                current_cursor_value = datetime.datetime.fromisoformat(self.state[self.cursor_field])
                latest_cursor_value = datetime.datetime.fromisoformat(modified_time)
                new_cursor_value = max(latest_cursor_value, current_cursor_value)
                self.state = {self.cursor_field: new_cursor_value.isoformat("T", "seconds")}
            yield record

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        last_modified = stream_state.get(self.cursor_field, self._start_datetime)
        # since API filters inclusively, we add 1 sec to prevent duplicate reads
        last_modified_dt = datetime.datetime.fromisoformat(last_modified)
        last_modified_dt += datetime.timedelta(seconds=1)
        last_modified = last_modified_dt.isoformat("T", "seconds")
        return {"If-Modified-Since": last_modified}


class ZohoStreamFactory:
    def __init__(self, config: Mapping[str, Any]):
        self.api = ZohoAPI(config)
        self._config = config

    def _init_modules_meta(self) -> List[ModuleMeta]:
        modules_meta_json = self.api.modules_settings()
        modules = [ModuleMeta.from_dict(module) for module in modules_meta_json]
        return list(filter(lambda module: module.api_supported, modules))

    def _populate_fields_meta(self, module: ModuleMeta):
        logger.info(
            "Processing module api_name=%s module_name=%s",
            module.api_name,
            module.module_name,
        )
        fields_meta_json, fields_metadata_unavailable, http_status = self.api.fields_settings(module.api_name)
        module.fields_metadata_unavailable = fields_metadata_unavailable
        fields_meta = []
        for field in fields_meta_json:
            if "length" not in field:
                logger.info(
                    "Field metadata missing length for module api_name=%s field api_name=%s data_type=%s display_label=%s",
                    module.api_name,
                    field.get("api_name"),
                    field.get("data_type"),
                    field.get("display_label"),
                )
            pick_list_values = field.get("pick_list_values", [])
            if pick_list_values:
                field["pick_list_values"] = [ZohoPickListItem.from_dict(pick_list_item) for pick_list_item in pick_list_values]
            fields_meta.append(FieldMeta.from_dict(field))
        module.fields = fields_meta
        logger.info(
            "Module metadata populated api_name=%s module_name=%s http_status=%s fields_count=%s metadata_unavailable=%s",
            module.api_name,
            module.module_name,
            http_status,
            len(fields_meta),
            fields_metadata_unavailable,
        )

    def _populate_module_meta(self, module: ModuleMeta):
        module_meta_json = self.api.module_settings(module.api_name)
        module_meta = next(iter(module_meta_json), None)
        if module_meta:
            module.update_from_dict(module_meta)
        else:
            logger.warning(
                "Module metadata unavailable for api_name=%s module_name=%s; continuing with modules list metadata",
                module.api_name,
                module.module_name,
            )

    def produce(self) -> List[HttpStream]:
        modules = self._init_modules_meta()
        streams = []

        def populate_module(module):
            self._populate_module_meta(module)
            self._populate_fields_meta(module)

        def chunk(max_len, lst):
            for i in range(math.ceil(len(lst) / max_len)):
                yield lst[i * max_len : (i + 1) * max_len]

        max_concurrent_request = self.api.max_concurrent_requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent_request) as executor:
            for batch in chunk(max_concurrent_request, modules):
                list(executor.map(lambda module: populate_module(module), batch))

        bases = (IncrementalZohoCrmStream,)
        for module in modules:
            stream_cls_attrs = {"url_base": self.api.api_url, "module": module}
            stream_cls_name = f"Incremental{module.api_name}ZohoCRMStream"
            incremental_stream_cls = type(stream_cls_name, bases, stream_cls_attrs)
            stream = incremental_stream_cls(self.api.authenticator, config=self._config)
            schema = stream.get_json_schema()
            stream_created = schema is not None
            fields_count = len(list(module.fields or []))
            fallback_schema_used = stream_created and module.fields_metadata_unavailable and fields_count == 0

            logger.info(
                "DISCOVER module api_name=%s module_name=%s fields_count=%s metadata_unavailable=%s "
                "schema_created=%s stream_created=%s",
                module.api_name,
                module.module_name,
                fields_count,
                module.fields_metadata_unavailable,
                schema is not None,
                stream_created,
            )

            if is_deals_module(module.api_name, module.module_name):
                logger.info(
                    "REAL DEALS DEBUG: api_name=%s module_name=%s fields_count=%s metadata_unavailable=%s "
                    "fallback_schema_used=%s stream_created=%s",
                    module.api_name,
                    module.module_name,
                    fields_count,
                    module.fields_metadata_unavailable,
                    fallback_schema_used,
                    stream_created,
                )

            if schema:
                streams.append(stream)

        streams.extend(self._produce_deleted_streams(modules))
        return streams

    def _produce_deleted_streams(self, modules: List[ModuleMeta]) -> List[HttpStream]:
        deleted_streams: List[HttpStream] = []
        for module in modules:
            if not is_deleted_stream_candidate(module.api_name):
                continue
            if not self.api.supports_deleted_records(module.api_name):
                continue

            stream_cls_name = f"IncrementalDeleted{module.api_name}ZohoCRMStream"
            deleted_stream_cls = type(
                stream_cls_name,
                (DeletedZohoCrmStream,),
                {"url_base": self.api.api_url, "module": module},
            )
            stream = deleted_stream_cls(self.api.authenticator, config=self._config)
            logger.info(
                "Deleted stream added: module_api_name=%s stream=%s",
                module.api_name,
                stream.name,
            )
            deleted_streams.append(stream)
        return deleted_streams
