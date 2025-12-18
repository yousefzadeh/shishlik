select
ptc.TenantId,
ptc.ProjectTaskId Task_Subtask_Id,
ptc.ControlId Task_Subtask_LinkedControlId,
c.Controls_Name Task_Subtask_LinkedControl

from {{ source("project_ref_models", "ProjectTaskControlStatement") }} ptc
join {{ ref("vControls") }} c
on c.Controls_Id = ptc.ControlId
where ptc.IsDeleted = 0
and ptc.StatementId is null