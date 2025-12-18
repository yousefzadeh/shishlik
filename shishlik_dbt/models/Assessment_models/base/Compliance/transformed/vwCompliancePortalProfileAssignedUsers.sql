with base as (
select
cp.CompliancePortalProfile_TenantId,
cp.CompliancePortalProfile_Id,
cp.CompliancePortalProfile_Name
from {{ ref("vwCompliancePortalProfile") }} cp
)
, cp_own as (
select
cpo.CompliancePortalProfileOwner_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(cpo.CompliancePortalProfileOwner_LastModificationTime, cpo.CompliancePortalProfileOwner_CreationTime) as Record_LastModificationTime,
'Owner' Module,
'Trust Portal' AssignedItemType,
b.CompliancePortalProfile_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
join {{ ref("vwCompliancePortalProfileOwner") }} cpo
on cpo.CompliancePortalProfileOwner_CompliancePortalProfileId = b.CompliancePortalProfile_Id
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = cpo.CompliancePortalProfileOwner_UserId
where au.AbpUsers_FullName is not null
)
, cp_acm as (
select
cpo.CompliancePortalProfileAccessMember_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(cpo.CompliancePortalProfileAccessMember_LastModificationTime, cpo.CompliancePortalProfileAccessMember_CreationTime) as Record_LastModificationTime,
'Access Member' Module,
'Trust Portal' AssignedItemType,
b.CompliancePortalProfile_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
join {{ ref("vwCompliancePortalProfileAccessMember") }} cpo
on cpo.CompliancePortalProfileAccessMember_CompliancePortalProfileId = b.CompliancePortalProfile_Id
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = cpo.CompliancePortalProfileAccessMember_UserId
where au.AbpUsers_FullName is not null
)

, final as (
select
TenantId,
UserName,
UserEmail,
Record_LastModificationTime,
Module,
AssignedItemType,
AssignedItemName,
AssignedItemParentType,
AssignedItemParentName
from cp_own

union all

select
TenantId,
UserName,
UserEmail,
Record_LastModificationTime,
Module,
AssignedItemType,
AssignedItemName,
AssignedItemParentType,
AssignedItemParentName
from cp_acm
)

select * from final