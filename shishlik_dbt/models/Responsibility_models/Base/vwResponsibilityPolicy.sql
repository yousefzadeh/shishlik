{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [PolicyId], [StatementId] ResponsibilityId, [TenantId]
        from {{ source("statement_models", "StatementPolicy") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ResponsibilityPolicy") }},
    {{ col_rename("PolicyId", "ResponsibilityPolicy") }},
    {{ col_rename("ResponsibilityId", "ResponsibilityPolicy") }},
    {{ col_rename("TenantId", "ResponsibilityPolicy") }}
from base
