select
au.TenantId Tenant_Id,
t.Name Tenant_Name,
c.EntityId,
pc.Id,
c.ChangeTime Date_Time,
'User Added to Group' [Event],
au.Name+' '+au.Surname Impacted,
au.IsDeleted Impacted_IsDeleted,
au.Name+' '+au.Surname + ' added to group ' + aou.DisplayName Changed,
u.Name+' '+u.Surname Actioned_By,
u.IsDeleted Actioned_By_IsDeleted

from {{ source("assessment_models", "AbpUsers") }} au
join {{ source("assessment_models", "AbpTenants") }} t on t.Id = au.TenantId
join {{ source("assessment_models", "AbpUserOrganizationUnits") }} auou on au.Id = auou.UserId and au.TenantId = auou.TenantId
join {{ source("assessment_models", "AbpOrganizationUnits") }} aou on auou.OrganizationUnitId = aou.Id and auou.TenantId = aou.TenantId
join {{ source("assessment_models", "AbpEntityChanges") }} c on auou.Id = c.EntityId and auou.TenantId = c.TenantId
join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId and c.TenantId = pc.TenantId
join {{ source("assessment_models", "AbpUsers") }} u on u.Id = auou.CreatorUserId and u.TenantId = auou.TenantId
where pc.PropertyNameVarChar = 'IsDeleted' and pc.NewValue = 'false'
and c.EntityTypeFullName = 'Abp.Authorization.Users.UserOrganizationUnit'