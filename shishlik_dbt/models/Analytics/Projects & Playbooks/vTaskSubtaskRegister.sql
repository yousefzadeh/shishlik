select distinct
ptr.TenantId,
ptr.ProjectTaskId Task_Subtask_Id,
rr.Register_Id Task_Subtask_LinkedRegisterId,
r.Register_Name Task_Subtask_LinkedRegister

from {{ source("project_ref_models", "ProjectTaskRegisterItem") }} ptr
join {{ ref("vRegisterRecord") }} rr
on rr.Record_Id = ptr.RegisterItemId
join {{ ref("vRegister") }} r
on r.Register_Id = rr.Register_Id
and r.TenantId = rr.TenantId
where ptr.IsDeleted = 0