select
ptr.TenantId,
ptr.ProjectTaskId Task_Subtask_Id,
ptr.RegisterItemId Task_Subtask_LinkedIssueId,
i.Issues_Name Task_Subtask_LinkedIssue

from {{ source("project_ref_models", "ProjectTaskRegisterItem") }} ptr
join {{ ref("vIssues") }} i
on i.Issues_Id = ptr.RegisterItemId
where ptr.IsDeleted = 0