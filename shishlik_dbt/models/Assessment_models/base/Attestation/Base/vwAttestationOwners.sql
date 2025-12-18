{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [AttestationId], [UserId], [OrganizationUnitId], [TenantId]
        from {{ source("assessment_models", "AttestationOwners") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AttestationOwners") }},
    {{ col_rename("CreationTime", "AttestationOwners") }},
    {{ col_rename("LastModificationTime", "AttestationOwners") }},
    {{ col_rename("AttestationId", "AttestationOwners") }},
    {{ col_rename("UserId", "AttestationOwners") }},
    {{ col_rename("OrganizationUnitId", "AttestationOwners") }},

    {{ col_rename("TenantId", "AttestationOwners") }}
from base
