select distinct
au.TenantId Tenant_Id,
t.Name Tenant_Name,
aul.UserId EntityId,
au.Id,
DATEADD(mi, DATEDIFF(mi, 0, aul.CreationTime), 0) Date_Time,
'User Login' [Event],
au.Name+' '+au.Surname Impacted,
au.IsDeleted Impacted_IsDeleted,
au.Name+' '+au.Surname + ' logged in' Changed,
au.Name+' '+au.Surname Actioned_By,
au.IsDeleted Actioned_By_IsDeleted

from {{ source("assessment_models", "AbpUsers") }} au
join {{ source("assessment_models", "AbpTenants") }} t on t.Id = au.TenantId
join {{ source("assessment_models", "AbpUserLoginAttempts") }} aul on aul.UserId = au.Id and aul.TenantId = au.TenantId