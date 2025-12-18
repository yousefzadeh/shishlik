select
c.TenantId Tenant_Id,
t.Name Tenant_Name,
c.EntityId,
pc.Id,
c.ChangeTime Date_Time,
'Role Edited' [Event],
ar.DisplayName Impacted,
ar.IsDeleted Impacted_IsDeleted,
ar.DisplayName + ' edited: permission ' + pc2.NewValue + ' added' Changed,
au.Name+' '+au.Surname Actioned_By,
au.IsDeleted Actioned_By_IsDeleted

from {{ source("assessment_models", "AbpEntityChanges") }} c
join {{ source("assessment_models", "AbpTenants") }} t on t.Id = c.TenantId
join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on pc.EntityChangeId = c.Id and pc.TenantId = c.TenantId
join {{ source("assessment_models", "AbpEntityChangeSets") }} acs on acs.Id = c.EntityChangeSetId and acs.TenantId = c.TenantId
join {{ source("assessment_models", "AbpUsers") }} au on au.Id = acs.UserId and au.TenantId = acs.TenantId
join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc3 on pc3.EntityChangeId = c.Id and pc3.TenantId = c.TenantId
join {{ source("assessment_models", "AbpRoles") }} ar on ar.Id = pc3.NewValue and ar.TenantId = pc3.TenantId and pc3.PropertyNameVarChar = 'RoleId'
join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc2 on pc2.EntityChangeId = c.Id and pc2.TenantId = c.TenantId and pc2.PropertyNameVarChar = 'Name'
where pc.PropertyNameVarChar = 'IsGranted' and pc.NewValue = 'true'