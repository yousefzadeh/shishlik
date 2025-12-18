{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [AttestationId], [UserId], [OrganizationUnitId], [TenantId]
        from {{ source("assessment_models", "AttestationAccessMember") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AttestationAccessMember") }},
    {{ col_rename("CreationTime", "AttestationAccessMember") }},
    {{ col_rename("LastModificationTime", "AttestationAccessMember") }},
    {{ col_rename("AttestationId", "AttestationAccessMember") }},
    {{ col_rename("UserId", "AttestationAccessMember") }},
    {{ col_rename("OrganizationUnitId", "AttestationAccessMember") }},

    {{ col_rename("TenantId", "AttestationAccessMember") }}
from base
