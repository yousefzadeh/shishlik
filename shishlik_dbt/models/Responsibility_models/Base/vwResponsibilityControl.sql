{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [ControlId], [StatementId] ResponsibilityId, [TenantId]
        from {{ source("statement_models", "StatementControl") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ResponsibilityControl") }},
    {{ col_rename("ControlId", "ResponsibilityControl") }},
    {{ col_rename("ResponsibilityId", "ResponsibilityControl") }},
    {{ col_rename("TenantId", "ResponsibilityControl") }}
from base
