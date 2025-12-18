select
ptc.TenantId,
ptc.ProjectTaskId Task_Subtask_Id,
ptc.StatementId Task_SubTask_LinkedResponsibilityId,
cp.Responsibility_Name Task_SubTask_LinkedResponsibility

from {{ source("project_ref_models", "ProjectTaskControlStatement") }} ptc
join {{ ref("vControlResponsibility") }} cp
on cp.Responsibility_Id = ptc.StatementId
where ptc.IsDeleted = 0
and ptc.StatementId is not null