select
au.TenantId Tenant_Id,
t.Name Tenant_Name,
c.EntityId,
pc.Id,
DATEADD(mi, DATEDIFF(mi, 0, c.ChangeTime), 0) Date_Time,
'User Management' EventType,
'User Password Reset' [Event],
au.Name+' '+au.Surname Impacted,
au.IsDeleted Impacted_IsDeleted,
au.Name+' '+au.Surname + ' reset password' Changed,
au.Name+' '+au.Surname Actioned_By,
au.IsDeleted Actioned_By_IsDeleted

from {{ source("assessment_models", "AbpUsers") }} au
join {{ source("assessment_models", "AbpTenants") }} t on t.Id = au.TenantId
join {{ source("assessment_models", "AbpEntityChanges") }} c on c.EntityId = au.Id and c.TenantId = au.TenantId
join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on pc.EntityChangeId = c.Id and pc.TenantId = c.TenantId
where pc.PropertyNameVarChar = 'PasswordResetCode' and pc.OriginalValue is not Null