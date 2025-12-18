select
aou.TenantId Tenant_Id,
t.Name Tenant_Name,
c.EntityId,
pc.Id,
c.ChangeTime Date_Time,
'Group Edited' Event,
aou.DisplayName Impacted,
aou.IsDeleted Impacted_IsDeleted,
aou.DisplayName + ' edited: ' + au.Name+' '+au.Surname + ' removed' Changed,
au2.Name+' '+au2.Surname Actioned_By,
au2.IsDeleted Actioned_By_IsDeleted

from {{ source("assessment_models", "AbpOrganizationUnits") }} aou
join {{ source("assessment_models", "AbpTenants") }} t on t.Id = aou.TenantId
join {{ source("assessment_models", "AbpUserOrganizationUnits") }} auou on auou.OrganizationUnitId = aou.Id and auou.TenantId = aou.TenantId
join {{ source("assessment_models", "AbpUsers") }} au on au.Id = auou.UserId and au.TenantId = auou.TenantId
join {{ source("assessment_models", "AbpEntityChanges") }} c on auou.Id = c.EntityId and auou.TenantId = c.TenantId
join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId and c.TenantId = pc.TenantId
join {{ source("assessment_models", "AbpEntityChangeSets") }} aecs on aecs.Id = c.EntityChangeSetId and aecs.TenantId = c.TenantId
join {{ source("assessment_models", "AbpUsers") }} au2 on au2.Id = aecs.UserId and au2.TenantId = aecs.TenantId
where pc.PropertyNameVarChar = 'IsDeleted' and pc.NewValue = 'true'
and c.EntityTypeFullName = 'Abp.Authorization.Users.UserOrganizationUnit'