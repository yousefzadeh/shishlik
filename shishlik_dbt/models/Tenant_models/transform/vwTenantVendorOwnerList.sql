
with owner as (
    select
    OwnerType,
    Tenant_Id,
    TenantVendorOwner_TenantVendorId,
    TenantVendorOwner_UserOrgId,
    TenantVendorOwner_Name
    from {{ ref("vwTenantVendorOwnerName") }} 
),
final as (
    select 
    Tenant_Id,
    TenantVendorOwner_TenantVendorId TenantVendor_Id,
    string_agg(TenantVendorOwner_Name,', ') Owner_List
    from owner 
    group by 
    Tenant_Id,
    TenantVendorOwner_TenantVendorId
)
select
Tenant_Id,
TenantVendor_Id,
Owner_List
from final 
{# where Tenant_Id = 1384 #}