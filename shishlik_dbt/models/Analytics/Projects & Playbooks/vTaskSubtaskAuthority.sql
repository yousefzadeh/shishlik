select distinct
ptp.TenantId,
ptp.ProjectTaskId Task_Subtask_Id,
ap.Authority_Id Task_Subtask_LinkedAuthorityId,
a.Authority_Name Task_Subtask_LinkedAuthority

from {{ source("project_ref_models", "ProjectTaskProvision") }} ptp
join {{ ref("vAuthorityProvision") }} ap
on ap.AuthorityProvision_Id = ptp.AuthorityProvisionId
join {{ ref("vAuthority") }} a
on a.Authority_Id = ap.Authority_Id
where ptp.IsDeleted = 0