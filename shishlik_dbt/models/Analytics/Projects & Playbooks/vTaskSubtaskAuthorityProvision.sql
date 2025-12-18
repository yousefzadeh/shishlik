select
ptp.TenantId,
ptp.ProjectTaskId Task_Subtask_Id,
ptp.AuthorityProvisionId Task_Subtask_LinkedAuthorityProvisionId,
ap.AuthorityProvision_Name Task_Subtask_LinkedAuthorityProvision

from {{ source("project_ref_models", "ProjectTaskProvision") }} ptp
join {{ ref("vAuthorityProvision") }} ap
on ap.AuthorityProvision_Id = ptp.AuthorityProvisionId
where ptp.IsDeleted = 0