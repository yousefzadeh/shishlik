select
ar.TenantId Tenant_Id,
t.Name Tenant_Name,
c.EntityId,
pc.Id,
c.ChangeTime Date_Time,
'Role Deleted' Event,
ar.DisplayName Impacted,
ar.IsDeleted Impacted_IsDeleted,
ar.DisplayName + ' deleted' Changed,
au.Name+' '+au.Surname Actioned_By,
au.IsDeleted Actioned_By_IsDeleted

from {{ source("assessment_models", "AbpRoles") }} ar
join {{ source("assessment_models", "AbpTenants") }} t on t.Id = ar.TenantId
join {{ source("assessment_models", "AbpEntityChanges") }} c on ar.Id = c.EntityId and ar.TenantId = c.TenantId
join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId and c.TenantId = pc.TenantId
join {{ source("assessment_models", "AbpUsers") }} au on au.Id = ar.DeleterUserId
where pc.PropertyNameVarChar = 'IsDeleted' and pc.NewValue = 'true'