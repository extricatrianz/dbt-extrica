from dataclasses import dataclass
from typing import Dict, Optional

import agate
from dbt.adapters.base.impl import AdapterConfig, ConstraintSupport
from dbt.adapters.capability import (
    Capability,
    CapabilityDict,
    CapabilitySupport,
    Support,
)
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.graph.nodes import ConstraintType
from dbt.exceptions import DbtDatabaseError

from dbt.adapters.extrica import ExtricaColumn, ExtricaConnectionManager, ExtricaRelation


@dataclass
class ExtricaConfig(AdapterConfig):
    properties: Optional[Dict[str, str]] = None
    view_security: Optional[str] = "definer"


class ExtricaAdapter(SQLAdapter):
    Relation = ExtricaRelation
    Column = ExtricaColumn
    ConnectionManager = ExtricaConnectionManager
    AdapterSpecificConfigs = ExtricaConfig

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    _capabilities: CapabilityDict = CapabilityDict(
        {
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
            # No information about last table modification in information_schema.tables
            Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Unsupported),
        }
    )

    @classmethod
    def date_function(cls):
        return "datenow()"

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "VARCHAR"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "DOUBLE" if decimals else "INTEGER"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "TIMESTAMP"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "DATE"

    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        return f"{add_to} + interval '{number}' {interval}"

    def get_columns_in_relation(self, relation):
        try:
            return super().get_columns_in_relation(relation)
        except DbtDatabaseError as exc:
            if "does not exist" in str(exc):
                return []
            else:
                raise

    def valid_incremental_strategies(self):
        return ["append", "merge", "delete+insert"]
