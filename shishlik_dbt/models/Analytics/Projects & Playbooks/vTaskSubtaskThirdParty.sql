select
ptt.TenantId,
ptt.ProjectTaskId Task_Subtask_Id,
ptt.TenantVendorId Task_Subtask_LinkedThirdPartyId,
tp.ThirdParty_Name Task_Subtask_LinkedThirdParty

from {{ source("project_ref_models", "ProjectTaskThirdParty") }} ptt
join {{ ref("vThirdParty") }} tp
on tp.ThirdParty_Id = ptt.TenantVendorId
where ptt.IsDeleted = 0