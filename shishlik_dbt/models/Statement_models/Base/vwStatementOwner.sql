{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [StatementId], [UserId], [OrganizationUnitId]
        from {{ source("statement_models", "StatementOwner") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "StatementOwner") }},
    {{ col_rename("CreationTime", "StatementOwner") }},
    {{ col_rename("LastModificationTime", "StatementOwner") }},
    {{ col_rename("TenantId", "StatementOwner") }},
    {{ col_rename("StatementId", "StatementOwner") }},
    {{ col_rename("UserId", "StatementOwner") }},

    {{ col_rename("OrganizationUnitId", "StatementOwner") }}
from base
