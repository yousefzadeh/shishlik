select
aou.TenantId Tenant_Id,
t.Name Tenant_Name,
c.EntityId,
pc.Id,
c.ChangeTime Date_Time,
'Group Edited' [Event],
pc.OriginalValue Impacted,
1 Impacted_IsDeleted,
pc.OriginalValue + ' name edited to '+pc.NewValue Changed,
au.Name+' '+au.Surname Actioned_By,
au.IsDeleted Actioned_By_IsDeleted

from {{ source("assessment_models", "AbpOrganizationUnits") }} aou
join {{ source("assessment_models", "AbpTenants") }} t on t.Id = aou.TenantId
join {{ source("assessment_models", "AbpEntityChanges") }} c on aou.Id = c.EntityId and aou.TenantId = c.TenantId
join {{ source("assessment_models", "AbpEntityChangeSets") }} aecs on aecs.Id = c.EntityChangeSetId and aecs.TenantId = c.TenantId
join {{ source("assessment_models", "AbpUsers") }} au on au.Id = aecs.UserId and au.TenantId = aecs.TenantId
join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId and c.TenantId = pc.TenantId
where pc.PropertyNameVarChar = 'DisplayName' and pc.OriginalValue is not null