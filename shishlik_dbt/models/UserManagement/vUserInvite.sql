select 
t.Id Tenant_Id,
t.Name Tenant_Name,
c.EntityId,
pc.Id,
c.ChangeTime Date_Time,
'User Invite' [Event],
au.Name+' '+au.Surname Impacted,
au.IsDeleted Impacted_IsDeleted,
au.Name+' '+au.Surname + ' was invited' Changed,
u.Name+' '+u.Surname Actioned_By,
u.IsDeleted Actioned_By_IsDeleted

from {{ source("assessment_models", "AbpTenants") }} t
join {{ source("assessment_models", "AbpUsers") }} au on au.TenantId = t.Id
join {{ source("assessment_models", "AbpEntityChanges") }} c on c.EntityId = au.Id and c.TenantId = au.TenantId
join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on pc.EntityChangeId = c.Id and pc.TenantId = c.TenantId
join {{ source("assessment_models", "AbpUsers") }} u on u.Id = au.CreatorUserId and u.TenantId = au.TenantId
where pc.PropertyNameVarChar = 'IsActive' and pc.NewValue = 'true' and pc.OriginalValue is NULL