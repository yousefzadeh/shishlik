select 
au.TenantId Tenant_Id,
t.Name Tenant_Name,
c.EntityId,
pc.Id,
c.ChangeTime Date_Time,
'User Made Inactive' [Event],
au.Name+' '+au.Surname Impacted,
au.IsDeleted Impacted_IsDeleted,
au.Name+' '+au.Surname + ' made inactive' Changed,
u.Name+' '+u.Surname Actioned_By,
u.IsDeleted Actioned_By_IsDeleted

from {{ source("assessment_models", "AbpUsers") }} au
join {{ source("assessment_models", "AbpTenants") }} t on t.Id = au.TenantId
join {{ source("assessment_models", "AbpEntityChanges") }} c on c.EntityId = au.Id and c.TenantId = au.TenantId
join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on pc.EntityChangeId = c.Id and pc.TenantId = c.TenantId
join {{ source("assessment_models", "AbpEntityChangeSets") }} acs on acs.Id = c.EntityChangeSetId and acs.TenantId = c.TenantId
join {{ source("assessment_models", "AbpUsers") }} u on u.Id = acs.UserId and u.TenantId = acs.TenantId
where pc.PropertyNameVarChar = 'IsActive' and pc.NewValue = 'false'