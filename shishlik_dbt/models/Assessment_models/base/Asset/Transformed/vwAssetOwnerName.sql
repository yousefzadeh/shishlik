
with owner_user as (
    select
    'User Owner' as OwnerType,
    AssetOwner_Id,
    AssetOwner_TenantId Tenant_Id,
    AssetOwner_AssetId,
    AssetOwner_UserId AssetOwner_UserOrgId,
    AbpUsers_FullName AssetOwner_Name
    from {{ ref("vwAssetOwner") }} tvo 
    join {{ ref("vwAbpUser") }} u on tvo.AssetOwner_UserId = u.AbpUsers_Id
),
owner_org as (
    select
    'Org Owner' as OwnerType,
    AssetOwner_Id,
    AssetOwner_TenantId Tenant_Id,
    AssetOwner_AssetId,
    AssetOwner_OrganizationUnitId AssetOwner_UserOrgId,
    AbpOrganizationUnits_DisplayName AssetOwner_Name
    from {{ ref("vwAssetOwner") }} tvo 
    join {{ ref("vwAbpOrganizationUnits") }} o on tvo.AssetOwner_OrganizationUnitId = o.AbpOrganizationUnits_Id
),
final as (
    select * from owner_user 
    union all 
    select * from owner_org
)
select
OwnerType,
Tenant_Id,
AssetOwner_AssetId,
AssetOwner_UserOrgId,
AssetOwner_Name
from final 
{# where Tenant_Id = 1384 #}