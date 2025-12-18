with ap as (
select
pc.TenantId,
t.Name Tenant_Name,
c.EntityId,
pc.Id,
c.Id EntityChangeId,
pc.PropertyNameVarChar Property,
pc.NewValue PropertyValue,
c.ChangeTime,
u.Name+' '+u.Surname Actioned_By,
u.IsDeleted Actioned_By_IsDeleted

from {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc
join {{ source("assessment_models", "AbpTenants") }} t on t.Id = pc.TenantId
join {{ source("assessment_models", "AbpEntityChanges") }} c on c.Id = pc.EntityChangeId
join {{ source("assessment_models", "AbpEntityChangeSets") }} cs on cs.Id = c.EntityChangeSetId
join {{ source("assessment_models", "AbpUsers") }} u on u.Id = cs.UserId
where c.EntityTypeFullName = 'Abp.Authorization.Users.UserPermissionSetting'
)
, imp as (
select ap.TenantId, ap.Id, ap.EntityId, ap.EntityChangeId, au.Name+' '+au.Surname Impacted, au.IsDeleted Impacted_IsDeleted
from ap
join {{ source("assessment_models", "AbpUsers") }} au on au.Id = ap.PropertyValue
where ap.Property = 'UserId' and au.IsInvitedAdvisor = 1
)
, prm as (
select ap.TenantId, ap.Id, ap.EntityId, ap.EntityChangeId, REPLACE(ap.PropertyValue,'"','') PermissionName
from ap
where ap.Property = 'Name'
)
, usr_prm as (
select
ap.TenantId Tenant_Id,
ap.Tenant_Name,
ap.EntityId,
ap.Id, 
ap.ChangeTime Date_Time,
'Advisor Permission Added' [Event],
imp.Impacted,
imp.Impacted_IsDeleted,
imp.Impacted + ' assigned permission ' + prm.PermissionName Changed,
ap.Actioned_By,
ap.Actioned_By_IsDeleted

from ap
join imp on ap.EntityChangeId = imp.EntityChangeId
join prm on ap.EntityChangeId = prm.EntityChangeId
where ap.Property = 'IsGranted' and ap.PropertyValue = 'true'

union all

select
ap.TenantId Tenant_Id,
ap.Tenant_Name,
ap.EntityId,
ap.Id, 
ap.ChangeTime Date_Time,
'Advisor Permission Removed' [Event],
imp.Impacted,
imp.Impacted_IsDeleted,
imp.Impacted + ' unassigned permission ' + prm.PermissionName Changed,
ap.Actioned_By,
ap.Actioned_By_IsDeleted

from ap
join imp on ap.EntityChangeId = imp.EntityChangeId
join prm on ap.EntityChangeId = prm.EntityChangeId
where ap.Property = 'IsGranted' and ap.PropertyValue = 'false'
)

select
Tenant_Id,
Tenant_Name,
EntityId,
Id, 
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from usr_prm