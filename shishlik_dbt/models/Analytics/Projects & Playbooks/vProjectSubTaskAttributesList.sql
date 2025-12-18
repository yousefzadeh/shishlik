with prj_subtsk as (
select
pst.TenantId,
pst.Project_Id,
pst.ProjectTask_Id,
pst.ProjectSubTask_Id,
pst.ProjectSubTask_Name
from {{ ref("vProjectSubTask") }} pst
)
, assignee as (
select
pta.TenantId, pta.ProjectTask_Id, string_agg(pta.ProjectTask_Assignee , '; ') ProjectSubTask_AssigneeList
from {{ ref("vProjectTaskAssignee") }} pta
group by
pta.TenantId, pta.ProjectTask_Id
)
, doc as (
select
ptd.TenantId, ptd.ProjectTask_Id, string_agg(ptd.ProjectTask_DocumentName , '; ') ProjectSubTask_DocumentList
from {{ ref("vProjectTaskDocument") }} ptd
group by
ptd.TenantId, ptd.ProjectTask_Id
)

select
pst.TenantId,
pst.Project_Id,
pst.ProjectTask_Id,
pst.ProjectSubTask_Id,
pst.ProjectSubTask_Name,
a.ProjectSubTask_AssigneeList,
d.ProjectSubTask_DocumentList

from prj_subtsk pst
left join assignee a on a.ProjectTask_Id = pst.ProjectSubTask_Id
left join doc d on d.ProjectTask_Id = pst.ProjectSubTask_Id