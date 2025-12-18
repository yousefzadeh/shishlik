{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [ControlId], [StatementId], [TenantId]
        from {{ source("statement_models", "StatementControl") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "StatementControl") }},
    {{ col_rename("ControlId", "StatementControl") }},
    {{ col_rename("StatementId", "StatementControl") }},
    {{ col_rename("TenantId", "StatementControl") }}
from base
