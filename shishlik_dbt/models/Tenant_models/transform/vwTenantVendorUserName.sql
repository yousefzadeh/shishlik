
with member_user as (
    select
    'User' as MemberType,
    TenantVendorUser_Id,
    TenantVendorUser_TenantId Tenant_Id,
    TenantVendorUser_TenantVendorId,
    TenantVendorUser_UserId TenantVendorUser_UserOrgId,
    AbpUsers_FullName TenantVendorUser_Name
    from {{ ref("vwTenantVendorUser") }} tvo 
    join {{ ref("vwAbpUser") }} u on tvo.TenantVendorUser_UserId = u.AbpUsers_Id
)
select
MemberType,
Tenant_Id,
TenantVendorUser_TenantVendorId,
TenantVendorUser_UserOrgId,
TenantVendorUser_Name
from member_user 
{# where Tenant_Id = 1384 #}