{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [StatementId], [UserId], [OrganizationUnitId]
        from {{ source("statement_models", "StatementMember") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "StatementMember") }},
    {{ col_rename("CreationTime", "StatementMember") }},
    {{ col_rename("LastModificationTime", "StatementMember") }},
    {{ col_rename("TenantId", "StatementMember") }},
    {{ col_rename("StatementId", "StatementMember") }},
    {{ col_rename("UserId", "StatementMember") }},

    {{ col_rename("OrganizationUnitId", "StatementMember") }}
from base
