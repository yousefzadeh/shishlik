{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [PolicyId], [StatementId], [TenantId]
        from {{ source("statement_models", "StatementPolicy") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "StatementPolicy") }},
    {{ col_rename("PolicyId", "StatementPolicy") }},
    {{ col_rename("StatementId", "StatementPolicy") }},
    {{ col_rename("TenantId", "StatementPolicy") }}
from base
