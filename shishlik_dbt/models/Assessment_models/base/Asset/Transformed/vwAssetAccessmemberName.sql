
with member_user as (
    select
    'User Owner' as MemberType,
    AssetAccessMember_Id,
    AssetAccessMember_TenantId Tenant_Id,
    AssetAccessMember_AssetId,
    AssetAccessMember_UserId AssetAccessMember_UserOrgId,
    AbpUsers_FullName AssetAccessMember_Name
    from {{ ref("vwAssetAccessMember") }} tvo 
    join {{ ref("vwAbpUser") }} u on tvo.AssetAccessMember_UserId = u.AbpUsers_Id
),
member_org as (
    select
    'Org Owner' as MemberType,
    AssetAccessMember_Id,
    AssetAccessMember_TenantId Tenant_Id,
    AssetAccessMember_AssetId,
    AssetAccessMember_OrganizationUnitId AssetAccessMember_UserOrgId,
    AbpOrganizationUnits_DisplayName AssetAccessMember_Name
    from {{ ref("vwAssetAccessMember") }} tvo 
    join {{ ref("vwAbpOrganizationUnits") }} o on tvo.AssetAccessMember_OrganizationUnitId = o.AbpOrganizationUnits_Id
),
final as (
    select * from member_user 
    union all 
    select * from member_org
)
select
MemberType,
Tenant_Id,
AssetAccessMember_AssetId,
AssetAccessMember_UserOrgId,
AssetAccessMember_Name
from final 
{# where Tenant_Id = 1384 #}