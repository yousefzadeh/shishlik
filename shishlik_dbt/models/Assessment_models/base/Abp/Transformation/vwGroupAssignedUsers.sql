select
auou.AbpUserOrganizationUnits_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
auou.AbpUserOrganizationUnits_CreationTime as Record_LastModificationTime,
'Group Member' Module,
'Groups' AssignedItemType,
aou.AbpOrganizationUnits_DisplayName AssignedItemName,
'Administration'+ ' > ' + 'Groups' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from {{ ref("vwAbpUserOrganizationUnits") }} auou
join {{ ref("vwAbpOrganizationUnits") }} aou
on aou.AbpOrganizationUnits_Id = auou.AbpUserOrganizationUnits_OrganizationUnitId
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = auou.AbpUserOrganizationUnits_UserId