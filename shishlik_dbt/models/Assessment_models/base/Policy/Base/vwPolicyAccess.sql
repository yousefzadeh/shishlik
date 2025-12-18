{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [LastAccessed], [PolicyId], [UserId], [TenantId]
        from {{ source("assessment_models", "PolicyAccess") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "PolicyAccess") }},
    {{ col_rename("LastAccessed", "PolicyAccess") }},
    {{ col_rename("PolicyId", "PolicyAccess") }},
    {{ col_rename("UserId", "PolicyAccess") }},

    {{ col_rename("TenantId", "PolicyAccess") }}
from base
