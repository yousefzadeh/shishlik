
with owner_user as (
    select
    'User Owner' as OwnerType,
    TenantVendorOwner_Id,
    TenantVendorOwner_TenantId Tenant_Id,
    TenantVendorOwner_TenantVendorId,
    TenantVendorOwner_UserId TenantVendorOwner_UserOrgId,
    AbpUsers_FullName TenantVendorOwner_Name
    from {{ ref("vwTenantVendorOwner") }} tvo 
    join {{ ref("vwAbpUser") }} u on tvo.TenantVendorOwner_UserId = u.AbpUsers_Id
),
owner_org as (
    select
    'Org Owner' as OwnerType,
    TenantVendorOwner_Id,
    TenantVendorOwner_TenantId Tenant_Id,
    TenantVendorOwner_TenantVendorId,
    TenantVendorOwner_OrganizationUnitId TenantVendorOwner_UserOrgId,
    AbpOrganizationUnits_DisplayName TenantVendorOwner_Name
    from {{ ref("vwTenantVendorOwner") }} tvo 
    join {{ ref("vwAbpOrganizationUnits") }} o on tvo.TenantVendorOwner_OrganizationUnitId = o.AbpOrganizationUnits_Id
),
final as (
    select * from owner_user 
    union all 
    select * from owner_org
)
select
OwnerType,
Tenant_Id,
TenantVendorOwner_TenantVendorId,
TenantVendorOwner_UserOrgId,
TenantVendorOwner_Name
from final 
{# where Tenant_Id = 1384 #}