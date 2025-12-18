with prj_tsk as (
select
pt.TenantId,
pt.Project_Id,
pt.ProjectTask_Id,
pt.ProjectTask_Name
from {{ ref("vProjectTask") }} pt
)
, assignee as (
select
pta.TenantId, pta.ProjectTask_Id, string_agg(pta.ProjectTask_Assignee , '; ') ProjectTask_AssigneeList
from {{ ref("vProjectTaskAssignee") }} pta
group by
pta.TenantId, pta.ProjectTask_Id
)
, doc as (
select
ptd.TenantId, ptd.ProjectTask_Id, string_agg(ptd.ProjectTask_DocumentName , '; ') ProjectTask_DocumentList
from {{ ref("vProjectTaskDocument") }} ptd
group by
ptd.TenantId, ptd.ProjectTask_Id
)

select
pt.TenantId,
pt.Project_Id,
pt.ProjectTask_Id,
pt.ProjectTask_Name,
a.ProjectTask_AssigneeList,
d.ProjectTask_DocumentList

from prj_tsk pt
left join assignee a on a.ProjectTask_Id = pt.ProjectTask_Id
left join doc d on d.ProjectTask_Id = pt.ProjectTask_Id