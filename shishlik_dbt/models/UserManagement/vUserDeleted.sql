select 
t.Id Tenant_Id,
t.Name Tenant_Name,
c.EntityId,
pc.Id,
c.ChangeTime Date_Time,
'User Deleted' [Event],
au.Name+' '+au.Surname Impacted,
au.IsDeleted Impacted_IsDeleted,
au.Name+' '+au.Surname + ' was deleted' Changed,
u.Name+' '+u.Surname Actioned_By,
u.IsDeleted Actioned_By_IsDeleted

from {{ source("assessment_models", "AbpTenants") }} t
join {{ source("assessment_models", "AbpUsers") }} au on au.TenantId = t.Id
join {{ source("assessment_models", "AbpEntityChanges") }} c on c.EntityId = au.Id and c.TenantId = au.TenantId
join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on pc.EntityChangeId = c.Id and pc.TenantId = c.TenantId
join {{ source("assessment_models", "AbpUsers") }} u on u.Id = au.DeleterUserId and u.TenantId = au.TenantId
where pc.PropertyNameVarChar = 'IsDeleted' and pc.NewValue = 'true'