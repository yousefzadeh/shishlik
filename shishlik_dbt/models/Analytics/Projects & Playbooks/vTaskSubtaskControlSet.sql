select distinct
ptc.TenantId,
ptc.ProjectTaskId Task_Subtask_Id,
cs.ControlSet_Id Task_Subtask_LinkedControlSetId,
cs.ControlSet_Name Task_Subtask_LinkedControlSet

from {{ source("project_ref_models", "ProjectTaskControlStatement") }} ptc
join {{ ref("vControls") }} c
on c.Controls_Id = ptc.ControlId
join {{ ref("vPolicyDomain") }} pd
on pd.PolicyDomain_Id = c.PolicyDomain_Id
join {{ ref("vControlSet") }} cs
on cs.ControlSet_Id = pd.ControlSet_Id
where ptc.IsDeleted = 0
and ptc.StatementId is null