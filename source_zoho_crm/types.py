#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import copy
import dataclasses
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Union

from .exceptions import IncompleteMetaDataException, UnknownDataTypeException


DEALS_MODULE_API_NAMES = frozenset({"Deals", "Potentials"})

DELETED_RECORDS_SUPPORTED_API_NAMES = frozenset(
    {
        "Leads",
        "Accounts",
        "Contacts",
        "Deals",
        "Potentials",
        "Campaigns",
        "Tasks",
        "Cases",
        "Events",
        "Calls",
        "Solutions",
        "Products",
        "Vendors",
        "Price_Books",
        "Quotes",
        "Sales_Orders",
        "Purchase_Orders",
        "Invoices",
        "Appointments",
        "Services",
        "Activities",
    }
)

DELETED_RECORDS_UNSUPPORTED_API_NAMES = frozenset(
    {
        "Attachments",
        "Notes",
        "Emails",
    }
)


def is_deals_module(api_name: str, module_name: str = "") -> bool:
    if api_name in DEALS_MODULE_API_NAMES:
        return True
    return module_name in DEALS_MODULE_API_NAMES


@dataclasses.dataclass
class Schema:
    description: str
    properties: Dict[str, Any]
    schema: str = "http://json-schema.org/draft-07/schema#"
    type: str = "object"
    additionalProperties: Any = True
    required: Optional[List[str]] = dataclasses.field(default_factory=list)


class ZohoBaseType(Enum):
    @classmethod
    def all(cls) -> List[str]:
        return list(map(lambda f: f.value, cls))

    def __eq__(self, other: object) -> bool:
        if type(other) is type(self):
            return super().__eq__(other)
        if type(other) == str:
            return self.value == other
        raise NotImplementedError(f"Type Mismatch: Enum and {type(other).__name__}")


class ZohoJsonType(ZohoBaseType):
    string = "string"
    integer = "integer"
    double = "double"
    boolean = "boolean"
    array = "jsonarray"
    object = "jsonobject"


class ZohoDataType(ZohoBaseType):
    textarea = "textarea"
    event_reminder = "event_reminder"
    phone = "phone"
    text = "text"
    profileimage = "profileimage"
    picklist = "picklist"
    bigint = "bigint"
    website = "website"
    email = "email"
    date = "date"
    datetime = "datetime"
    integer = "integer"
    currency = "currency"
    double = "double"
    boolean = "boolean"
    lookup = "lookup"
    ownerlookup = "ownerlookup"
    autonumber = "autonumber"
    multiselectpicklist = "multiselectpicklist"
    RRULE = "RRULE"
    ALARM = "ALARM"

    @classmethod
    def numeric_string_types(cls) -> Iterable["ZohoDataType"]:
        return cls.autonumber, cls.bigint


class FromDictMixin:
    @classmethod
    def _field_names(cls) -> Iterable[str]:
        return [field.name for field in dataclasses.fields(cls)]

    @classmethod
    def _filter_by_names(cls, dct: Dict[Any, Any]) -> Dict[Any, Any]:
        return {key: val for key, val in dct.items() if key in cls._field_names()}

    @classmethod
    def from_dict(cls, dct: MutableMapping[Any, Any]) -> object:
        return cls(**cls._filter_by_names(dct))

    def update_from_dict(self, dct: MutableMapping[Any, Any]):
        for key, val in self._filter_by_names(dct).items():
            setattr(self, key, val)


@dataclasses.dataclass
class ZohoPickListItem(FromDictMixin):
    display_value: str
    actual_value: str


@dataclasses.dataclass
class AutoNumberDict(FromDictMixin):
    prefix: str
    suffix: str


FieldType = Dict[Any, Any]

MODIFIED_TIME_SCHEMA_PROPERTY: FieldType = {"type": ["null", "string"], "format": "date-time"}
DELETED_TIME_SCHEMA_PROPERTY: FieldType = {"type": ["null", "string"], "format": "date-time"}

ZOHO_RECORDS_MAX_FIELDS_PER_REQUEST = 50
ZOHO_V8_RECORDS_PER_PAGE = 200
ZOHO_V8_MAX_PAGE_NUMBER = 10

DEALS_MANDATORY_RECORD_FIELDS = (
    "id",
    "Deal_Name",
    "Stage",
    "Pipeline",
    "Owner",
    "Created_Time",
    "Modified_Time",
)


@dataclasses.dataclass
class FieldMeta(FromDictMixin):
    json_type: str
    api_name: str
    data_type: str
    decimal_place: Optional[int]
    system_mandatory: bool
    display_label: str
    pick_list_values: Optional[List[ZohoPickListItem]]
    length: Optional[int] = None
    auto_number: Optional[AutoNumberDict] = dataclasses.field(default_factory=lambda: AutoNumberDict(prefix="", suffix=""))

    def _default_type_kwargs(self) -> Dict[str, str]:
        return {"title": self.display_label}

    def _picklist_items(self) -> Iterable[Union[str, None]]:
        default_list = [None]
        if not self.pick_list_values:
            return default_list
        return default_list + [pick_item.display_value for pick_item in self.pick_list_values]

    def _boolean_field(self) -> FieldType:
        return {"type": ["null", "boolean"], **self._default_type_kwargs()}

    def _integer_field(self) -> FieldType:
        return {"type": ["null", "integer"], **self._default_type_kwargs()}

    def _double_field(self) -> FieldType:
        typedef = {"type": ["null", "number"], **self._default_type_kwargs()}
        if self.decimal_place:
            typedef["multipleOf"] = float(Decimal("0.1") ** self.decimal_place)
        return typedef

    def _string_field(self) -> FieldType:
        if self.api_name == "Reminder":
            # this is a special case. although datatype = `picklist`,
            # actual values do not correspond to the values in the list
            return {"type": ["null", "string"], "format": "date-time", **self._default_type_kwargs()}

        typedef = {"type": ["null", "string"], **self._default_type_kwargs()}
        if self.length is not None:
            typedef["maxLength"] = self.length
        if self.data_type == ZohoDataType.website:
            typedef["format"] = "uri"
        elif self.data_type == ZohoDataType.email:
            typedef["format"] = "email"
        elif self.data_type == ZohoDataType.date:
            typedef["format"] = "date"
        elif self.data_type == ZohoDataType.datetime:
            typedef["format"] = "date-time"
        elif self.data_type == ZohoDataType.bigint:
            typedef["airbyte_type"] = "big_integer"
        elif self.data_type == ZohoDataType.autonumber:
            print(self.auto_number)
            if self.auto_number.get("prefix") or self.auto_number.get("suffix"):
                typedef["format"] = "string"
            else:
                typedef["airbyte_type"] = "big_integer"
        elif self.data_type == ZohoDataType.picklist and self.pick_list_values:
            typedef["enum"] = self._picklist_items()
        return typedef

    def _jsonarray_field(self) -> FieldType:
        typedef = {"type": "array", **self._default_type_kwargs()}
        if self.api_name in ("Product_Details", "Pricing_Details"):
            # these two fields are said to be text, but are actually complex objects
            typedef["items"] = {"type": "object"}
            return typedef
        if self.api_name == "Tag":
            # `Tag` is defined as string, but is actually an object
            typedef["items"] = {
                "type": "object",
                "additionalProperties": True,
                "required": ["name", "id"],
                "properties": {"name": {"type": "string"}, "id": {"type": "string"}},
            }
            return typedef
        if self.data_type in (ZohoDataType.text, *ZohoDataType.numeric_string_types()):
            typedef["items"] = {"type": "string"}
            if self.data_type == ZohoDataType.autonumber:
                if self.auto_number.get("prefix") or self.auto_number.get("suffix"):
                    typedef["items"]["format"] = "string"
                else:
                    typedef["items"]["airbyte_type"] = "big_integer"
            else:
                typedef["items"]["airbyte_type"] = "big_integer"
        if self.data_type == ZohoDataType.multiselectpicklist:
            typedef["minItems"] = 1
            typedef["uniqueItems"] = True
            items = {"type": ["null", "string"]}
            if self.pick_list_values:
                items["enum"] = self._picklist_items()
            typedef["items"] = items
        return typedef

    def _jsonobject_field(self) -> FieldType:
        lookup_typedef = {
            "type": ["null", "object"],
            "additionalProperties": True,
            "required": ["name", "id"],
            "properties": {"name": {"type": ["null", "string"]}, "id": {"type": "string"}},
            **self._default_type_kwargs(),
        }
        if self.data_type == ZohoDataType.lookup:
            return lookup_typedef
        if self.data_type == ZohoDataType.ownerlookup:
            owner_lookup_typedef = copy.deepcopy(lookup_typedef)
            owner_lookup_typedef["required"] += ["email"]
            owner_lookup_typedef["properties"]["email"] = {"type": "string", "format": "email"}
            return owner_lookup_typedef
        # exact specification unknown
        return {"type": ["null", "object"]}

    @property
    def schema(self) -> FieldType:
        if self.json_type in ZohoJsonType.all():
            return getattr(self, f"_{self.json_type}_field")()
        raise UnknownDataTypeException(f"JSON type: {self.json_type}, data type:{self.data_type}")


@dataclasses.dataclass
class ModuleMeta(FromDictMixin):
    api_name: str
    module_name: str
    api_supported: bool
    fields: Optional[Iterable[FieldMeta]] = dataclasses.field(default_factory=list)
    fields_metadata_unavailable: bool = False

    @property
    def schema(self) -> Schema:
        if not self.fields:
            if is_deals_module(self.api_name, self.module_name) and self.fields_metadata_unavailable:
                return build_deals_fallback_schema(self.module_name)
            raise IncompleteMetaDataException("Not enough data")
        required = ["id", "Modified_Time"] + [field_.api_name for field_ in self.fields if field_.system_mandatory]
        field_to_properties = {field_.api_name: field_.schema for field_ in self.fields}
        properties = {"id": {"type": "string"}, **field_to_properties}
        properties["Modified_Time"] = MODIFIED_TIME_SCHEMA_PROPERTY
        if is_deals_module(self.api_name, self.module_name):
            properties.update(_deals_mandatory_schema_properties())
        return Schema(description=self.module_name, properties=properties, required=required)


def _lookup_property(*, include_email: bool = False) -> FieldType:
    properties: Dict[str, Any] = {"name": {"type": ["null", "string"]}, "id": {"type": "string"}}
    required = ["name", "id"]
    if include_email:
        properties["email"] = {"type": "string", "format": "email"}
        required.append("email")
    return {
        "type": ["null", "object"],
        "additionalProperties": True,
        "required": required,
        "properties": properties,
    }


def _deals_mandatory_schema_properties() -> Dict[str, Any]:
    return {
        "Deal_Name": {"type": ["null", "string"]},
        "Stage": {"type": ["null", "string"]},
        "Pipeline": {"type": ["null", "string"]},
        "Owner": _lookup_property(include_email=True),
        "Created_Time": {"type": ["null", "string"], "format": "date-time"},
    }


def build_deals_fallback_schema(module_name: str = "Deals") -> Schema:
    properties: Dict[str, Any] = {
        "id": {"type": "string"},
        **_deals_mandatory_schema_properties(),
        "Contact_Name": _lookup_property(),
        "Account_Name": _lookup_property(),
        "Amount": {"type": ["null", "number"]},
        "Closing_Date": {"type": ["null", "string"], "format": "date"},
        "Type": {"type": ["null", "string"]},
        "Lead_Source": {"type": ["null", "string"]},
        "Modified_Time": MODIFIED_TIME_SCHEMA_PROPERTY,
    }
    return Schema(
        description=module_name,
        properties=properties,
        required=["id", "Deal_Name", "Stage", "Pipeline", "Owner", "Created_Time", "Modified_Time"],
    )


def collect_module_record_field_names(module: "ModuleMeta") -> List[str]:
    names: List[str] = ["id", "Modified_Time"]
    if module.fields:
        for field in module.fields:
            names.append(field.api_name)
    if is_deals_module(module.api_name, module.module_name):
        names.extend(DEALS_MANDATORY_RECORD_FIELDS)
    return list(dict.fromkeys(names))


def build_record_field_batches(
    field_names: List[str],
    mandatory_fields: Iterable[str],
    max_fields: int = ZOHO_RECORDS_MAX_FIELDS_PER_REQUEST,
) -> List[List[str]]:
    mandatory = list(dict.fromkeys(mandatory_fields))
    unique = list(dict.fromkeys(field_names))
    for name in mandatory:
        if name not in unique:
            unique.insert(0, name)

    if len(unique) <= max_fields:
        return [unique]

    batches: List[List[str]] = []
    first_batch = list(dict.fromkeys(mandatory))
    for name in unique:
        if name not in first_batch and len(first_batch) < max_fields:
            first_batch.append(name)
    batches.append(first_batch)

    used = set(first_batch)
    remaining = [name for name in unique if name not in used]
    chunk_size = max_fields - 1
    for offset in range(0, len(remaining), chunk_size):
        batch = ["id"] + remaining[offset : offset + chunk_size]
        batches.append(batch)
    return batches


def is_custom_module_api_name(api_name: str) -> bool:
    return api_name.endswith("__s") or api_name.endswith("__c")


def is_deleted_stream_candidate(api_name: str) -> bool:
    if api_name in DELETED_RECORDS_UNSUPPORTED_API_NAMES:
        return False
    if api_name in DELETED_RECORDS_SUPPORTED_API_NAMES:
        return True
    return is_custom_module_api_name(api_name)


def _deleted_user_property() -> FieldType:
    return {
        "type": ["null", "object"],
        "additionalProperties": True,
        "properties": {
            "id": {"type": "string"},
            "name": {"type": ["null", "string"]},
            "email": {"type": ["null", "string"], "format": "email"},
        },
    }


def build_deleted_record_schema(module_api_name: str) -> Schema:
    return Schema(
        description=f"Deleted records for {module_api_name}",
        properties={
            "id": {"type": "string"},
            "deleted_time": DELETED_TIME_SCHEMA_PROPERTY,
            "deleted_by": _deleted_user_property(),
            "created_by": _deleted_user_property(),
            "display_name": {"type": ["null", "string"]},
            "type": {"type": ["null", "string"]},
            "module_api_name": {"type": "string"},
        },
        required=["id", "module_api_name"],
    )
