select
ptr.TenantId,
ptr.ProjectTaskId Task_Subtask_Id,
ptr.RiskId Task_Subtask_LinkedRiskId,
r.Risk_Name Task_Subtask_LinkedRisk

from {{ source("project_ref_models", "ProjectTaskRisk") }} ptr
join {{ ref("vRisks") }} r
on r.Risk_Id = ptr.RiskId
where ptr.IsDeleted = 0