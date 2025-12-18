with
    base as (
        select
        Id,
        CreationTime,
        LastModificationTime,
        TenantId,
        ControlId,
        UserId,
        OrganizationUnitId
        from {{ source("assessment_models", "ControlOwner") }} {{ system_remove_IsDeleted() }}
    )
select
    {{ col_rename("Id", "ControlOwner") }},
    {{ col_rename("CreationTime", "ControlOwner") }},
    {{ col_rename("LastModificationTime", "ControlOwner") }},
    {{ col_rename("TenantId", "ControlOwner") }},
    {{ col_rename("ControlId", "ControlOwner") }},
    {{ col_rename("UserId", "ControlOwner") }},
    {{ col_rename("OrganizationUnitId", "ControlOwner") }}
from base

