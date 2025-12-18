select 
ai.TenantId Tenant_Id,
t.Name Tenant_Name,
ai.Id EntityId,
ai.Id,
ai.CreationTime Date_Time,
'User Management' EventType,
'Advisor Revoke' [Event],
ai.EmailAddress Impacted,
ai.IsDeleted Impacted_IsDeleted,
ai.EmailAddress + ' advisor revoked' Changed,
au.Name+' '+au.Surname Actioned_By,
au.IsDeleted Actioned_By_IsDeleted

from {{ source("assessment_models", "AdvisorInvite") }} ai
join {{ source("assessment_models", "AbpTenants") }} t on t.Id = ai.TenantId
join {{ source("assessment_models", "AbpUsers") }} au on au.Id = ai.DeleterUserId