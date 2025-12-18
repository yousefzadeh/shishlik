select
pta.TenantId,
pta.ProjectTaskId Task_Subtask_Id,
pta.AssessmentId Task_Subtask_LinkedAssessmentId,
a.Assessment_Name Task_Subtask_LinkedAssessment

from {{ source("project_ref_models", "ProjectTaskAssessment") }} pta
join {{ ref("vAssessment") }} a
on a.Assessment_Id = pta.AssessmentId
where pta.IsDeleted = 0