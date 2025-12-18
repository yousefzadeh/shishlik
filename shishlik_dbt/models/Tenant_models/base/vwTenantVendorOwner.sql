
with base as (
    select
    Id,
    TenantId,
    UserId,
    TenantVendorId,
    OrganizationUnitId,
    CreationTime,
    CreatorUserId,
    LastModificationTime,
    LastModifierUserId,
    IsDeleted,
    DeleterUserId,
    DeletionTime
    from {{ source("tenant_models", "TenantVendorOwner") }} tvo 
    where IsDeleted = 0
)
select
{{ col_rename("Id","TenantVendorOwner") }},
{{ col_rename("TenantId","TenantVendorOwner") }},
{{ col_rename("UserId","TenantVendorOwner") }},
{{ col_rename("TenantVendorId","TenantVendorOwner") }},
{{ col_rename("OrganizationUnitId","TenantVendorOwner") }},
{{ col_rename("CreationTime","TenantVendorOwner") }},
{{ col_rename("CreatorUserId","TenantVendorOwner") }},
{{ col_rename("LastModificationTime","TenantVendorOwner") }},
{{ col_rename("LastModifierUserId","TenantVendorOwner") }},
{{ col_rename("IsDeleted","TenantVendorOwner") }},
{{ col_rename("DeleterUserId","TenantVendorOwner") }},
{{ col_rename("DeletionTime","TenantVendorOwner") }}
from base
