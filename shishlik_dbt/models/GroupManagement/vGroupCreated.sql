select
aou.TenantId Tenant_Id,
t.Name Tenant_Name,
c.EntityId,
pc.Id,
c.ChangeTime Date_Time,
'Group Created' Event,
aou.DisplayName Impacted,
aou.IsDeleted Impacted_IsDeleted,
aou.DisplayName + ' created' Changed,
au.Name+' '+au.Surname Actioned_By,
au.IsDeleted Actioned_By_IsDeleted

from {{ source("assessment_models", "AbpOrganizationUnits") }} aou
join {{ source("assessment_models", "AbpTenants") }} t on t.Id = aou.TenantId
join {{ source("assessment_models", "AbpEntityChanges") }} c on aou.Id = c.EntityId and aou.TenantId = c.TenantId
join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId and c.TenantId = pc.TenantId
join {{ source("assessment_models", "AbpUsers") }} au on au.Id = aou.CreatorUserId
where pc.PropertyNameVarChar = 'IsDeleted' and pc.NewValue = 'false'