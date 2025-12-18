{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [StatementId] ResponsibilityId, [UserId], [OrganizationUnitId]
        from {{ source("statement_models", "StatementOwner") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ResponsibilityOwner") }},
    {{ col_rename("TenantId", "ResponsibilityOwner") }},
    {{ col_rename("ResponsibilityId", "ResponsibilityOwner") }},
    {{ col_rename("UserId", "ResponsibilityOwner") }},

    {{ col_rename("OrganizationUnitId", "ResponsibilityOwner") }}
from base
