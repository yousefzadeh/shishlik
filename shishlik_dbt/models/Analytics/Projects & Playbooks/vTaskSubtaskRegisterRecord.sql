select
ptr.TenantId,
ptr.ProjectTaskId Task_Subtask_Id,
ptr.RegisterItemId Task_Subtask_LinkedRegisterRecordId,
rr.Record_Name Task_Subtask_LinkedRegisterRecord

from {{ source("project_ref_models", "ProjectTaskRegisterItem") }} ptr
join {{ ref("vRegisterRecord") }} rr
on rr.Record_Id = ptr.RegisterItemId
where ptr.IsDeleted = 0