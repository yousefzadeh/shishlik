{{ config(materialized="view") }}
with
    base as (
        select Id, TenantId, Name, Description, Value, Scope, SourceType, ConditionJson,
        FormulaFieldType, FormulaFieldEntityType, Formula, FormulaFieldIdOrName, ValueLastUpdatedOn,
        LastUpdatedValue, ErrorState, Uuid
        from {{ source("metric_models", "Metric") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Metric") }},
    {{ col_rename("TenantId", "Metric") }},
    {{ col_rename("Name", "Metric") }},
    {{ col_rename("Description", "Metric") }},

    {{ col_rename("Value", "Metric") }},
    {{ col_rename("Scope", "Metric") }},
    {{ col_rename("SourceType", "Metric") }},
    {{ col_rename("ConditionJson", "Metric") }},
    {{ col_rename("FormulaFieldType", "Metric") }},

    {{ col_rename("FormulaFieldEntityType", "Metric") }},
    {{ col_rename("Formula", "Metric") }},
    {{ col_rename("FormulaFieldIdOrName", "Metric") }},
    {{ col_rename("ValueLastUpdatedOn", "Metric") }},
    {{ col_rename("LastUpdatedValue", "Metric") }},

    {{ col_rename("ErrorState", "Metric") }},
    {{ col_rename("Uuid", "Metric") }}
from base
