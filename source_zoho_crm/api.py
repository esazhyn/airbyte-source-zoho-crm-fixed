#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
from types import MappingProxyType
from typing import Any, List, Mapping, MutableMapping, Tuple
from urllib.parse import urlsplit, urlunsplit

import requests

from .auth import ZohoOauth2Authenticator
from .types import is_deals_module


logger = logging.getLogger(__name__)


class ZohoAPI:
    _DC_REGION_TO_ACCESS_URL = MappingProxyType(
        {
            "US": "https://accounts.zoho.com",
            "AU": "https://accounts.zoho.com.au",
            "EU": "https://accounts.zoho.eu",
            "IN": "https://accounts.zoho.in",
            "CN": "https://accounts.zoho.com.cn",
            "JP": "https://accounts.zoho.jp",
        }
    )
    _DC_REGION_TO_API_URL = MappingProxyType(
        {
            "US": "https://zohoapis.com",
            "AU": "https://zohoapis.com.au",
            "EU": "https://zohoapis.eu",
            "IN": "https://zohoapis.in",
            "CN": "https://zohoapis.com.cn",
            "JP": "https://zohoapis.jp",
        }
    )
    _API_ENV_TO_URL_PREFIX = MappingProxyType({"production": "", "developer": "developer", "sandbox": "sandbox"})
    _CONCURRENCY_API_LIMITS = MappingProxyType({"Free": 5, "Standard": 10, "Professional": 15, "Enterprise": 20, "Ultimate": 25})

    def __init__(self, config: Mapping[str, Any]):
        self.config = config
        self._authenticator = None

    @property
    def authenticator(self) -> ZohoOauth2Authenticator:
        if not self._authenticator:
            authenticator = ZohoOauth2Authenticator(
                f"{self._access_url}/oauth/v2/token", self.config["client_id"], self.config["client_secret"], self.config["refresh_token"]
            )
            self._authenticator = authenticator
        return self._authenticator

    @property
    def _access_url(self) -> str:
        return self._DC_REGION_TO_ACCESS_URL[self.config["dc_region"].upper()]

    @property
    def max_concurrent_requests(self) -> int:
        return self._CONCURRENCY_API_LIMITS[self.config["edition"]]

    @property
    def api_url(self) -> str:
        schema, domain, *_ = urlsplit(self._DC_REGION_TO_API_URL[self.config["dc_region"].upper()])
        prefix = self._API_ENV_TO_URL_PREFIX[self.config["environment"].lower()]
        if prefix:
            domain = f"{prefix}.{domain}"
        return urlunsplit((schema, domain, *_))

    def _json_from_path(self, path: str, key: str, params: MutableMapping[str, str] = None) -> List[MutableMapping[Any, Any]]:
        response = requests.get(url=f"{self.api_url}{path}", headers=self.authenticator.get_auth_header(), params=params or {})
        if response.status_code == 204:
            # Zoho CRM returns `No content` for Metadata of some modules
            logger.warning(f"{key.capitalize()} Metadata inaccessible: {response.content} [HTTP status {response.status_code}]")
            return []
        # Some Zoho CRM modules are listed globally but their individual
        # metadata endpoint omits the expected key (sometimes with an error
        # object). The stream factory can continue using the global listing.
        # Do not swallow errors on the GLOBAL module listing or other APIs.
        module_detail = key == "modules" and path.startswith("/crm/v2/settings/modules/")
        try:
            payload = response.json()
        except ValueError as exc:
            response.raise_for_status()
            raise ValueError(f"Invalid JSON in Zoho CRM metadata response for {path}") from exc

        if module_detail and response.status_code == 400 and isinstance(payload, dict) and payload.get("code") == "INVALID_MODULE":
            logger.warning("Individual module metadata unavailable for %s: HTTP 400 INVALID_MODULE; using module-list metadata", path)
            return []

        response.raise_for_status()
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected Zoho CRM metadata response type for {path}: {type(payload).__name__}")

        if key not in payload:
            if module_detail:
                logger.warning(
                    "Individual module metadata missing '%s' for %s: HTTP %s, code=%s; using module-list metadata",
                    key, path, response.status_code, payload.get("code", "unknown"),
                )
                return []
            raise ValueError(f"Zoho CRM metadata response for {path} missing required key '{key}'")

        result = payload[key]
        if not isinstance(result, list):
            raise ValueError(f"Zoho CRM metadata response for {path} has non-list '{key}' value")
        return result

    def module_settings(self, module_name: str) -> List[MutableMapping[Any, Any]]:
        return self._json_from_path(f"/crm/v2/settings/modules/{module_name}", key="modules")

    def modules_settings(self) -> List[MutableMapping[Any, Any]]:
        return self._json_from_path("/crm/v2/settings/modules", key="modules")

    def fields_settings(self, module_name: str) -> Tuple[List[MutableMapping[Any, Any]], bool, int]:
        logger.info("Requesting fields metadata for module api_name=%s", module_name)
        response = requests.get(
            url=f"{self.api_url}/crm/v2/settings/fields",
            headers=self.authenticator.get_auth_header(),
            params={"module": module_name},
        )
        http_status = response.status_code
        if http_status == 204:
            logger.warning(
                "Fields Metadata inaccessible for module api_name=%s: %s [HTTP status %s]",
                module_name,
                response.content,
                http_status,
            )
            return [], True, http_status
        response.raise_for_status()
        fields = response.json().get("fields", [])
        metadata_unavailable = is_deals_module(module_name) and len(fields) == 0
        if metadata_unavailable:
            logger.warning(
                "Fields metadata for module api_name=%s returned HTTP %s with empty fields array; "
                "Deals fallback eligibility enabled",
                module_name,
                http_status,
            )
        logger.info(
            "Fields metadata result for module api_name=%s: http_status=%s fields_count=%s metadata_unavailable=%s",
            module_name,
            http_status,
            len(fields),
            metadata_unavailable,
        )
        return fields, metadata_unavailable, http_status

    def check_connection(self) -> Tuple[bool, Any]:
        path = "/crm/v2/settings/modules"
        response = requests.get(url=f"{self.api_url}{path}", headers=self.authenticator.get_auth_header())
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            return False, exc.response.content
        return True, None

    def supports_deleted_records(self, module_api_name: str) -> bool:
        response = requests.get(
            url=f"{self.api_url}/crm/v8/{module_api_name}/deleted",
            headers=self.authenticator.get_auth_header(),
            params={"type": "all", "page": 1, "per_page": 1},
        )
        if response.status_code in (200, 204):
            logger.info("Deleted records API supported for module api_name=%s [HTTP status %s]", module_api_name, response.status_code)
            return True
        if response.status_code == 400:
            try:
                payload = response.json()
            except ValueError:
                payload = {}
            if payload.get("code") == "INVALID_MODULE" or "INVALID_MODULE" in str(payload):
                logger.warning(
                    "Deleted records API unsupported for module api_name=%s: %s",
                    module_api_name,
                    payload,
                )
                return False
        response.raise_for_status()
        return False
