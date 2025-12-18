with base as (
select
p.Project_TenantId,
p.Project_Id,
p.Project_Name,
p.Project_OwnerId,
coalesce(p.Project_LastModificationTime, p.Project_CreationTime) as Record_LastModificationTime
from {{ ref("vwProject") }} p
where p.Project_IsTemplate = 0
)
, proj_own as (
select 
b.Project_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
b.Record_LastModificationTime,
'Owner' Module,
'Projects' AssignedItemType,
b.Project_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = b.Project_OwnerId
where au.AbpUsers_FullName is not null
)
, proj_tsk_own as (
select 
b.Project_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(pta.ProjectTaskAssignee_LastModificationTime, pta.ProjectTaskAssignee_CreationTime) as Record_LastModificationTime,
'Owner' Module,
'Project Tasks' AssignedItemType,
b.Project_Name AssignedItemName,
'Projects'+ ' > ' + 'Project Tasks' AssignedItemParentType,
b.Project_Name+ ' > '+ pt.ProjectTask_Name AssignedItemParentName
from base b
join {{ ref("vwProjectTask") }} pt
on pt.ProjectTask_ProjectId = b.Project_Id
join {{ ref("vwProjectTaskAssignee") }} pta
on pta.ProjectTaskAssignee_ProjectTaskId = pt.ProjectTask_Id
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = pta.ProjectTaskAssignee_UserId
where au.AbpUsers_FullName is not null
)
, final as (
select
TenantId,
UserName,
UserEmail,
Record_LastModificationTime,
Module,
AssignedItemType,
AssignedItemName,
AssignedItemParentType,
AssignedItemParentName
from proj_own

union all

select
TenantId,
UserName,
UserEmail,
Record_LastModificationTime,
Module,
AssignedItemType,
AssignedItemName,
AssignedItemParentType,
AssignedItemParentName
from proj_tsk_own
)

select * from final