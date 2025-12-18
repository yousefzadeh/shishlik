select
ptr.TenantId,
ptr.ProjectTaskId Task_Subtask_Id,
ptr.RegisterItemId Task_Subtask_LinkedAssetId,
a.Asset_Name Task_Subtask_LinkedAsset

from {{ source("project_ref_models", "ProjectTaskRegisterItem") }} ptr
join {{ ref("vAsset") }} a
on a.Asset_Id = ptr.RegisterItemId
where ptr.IsDeleted = 0