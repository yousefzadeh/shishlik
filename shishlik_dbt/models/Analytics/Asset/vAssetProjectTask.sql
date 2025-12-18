select distinct
pti.TenantId,
pti.RegisterItemId Asset_Id,
pti.ProjectTaskId Asset_LinkedProjectTaskId,
pt.Name Asset_LinkedProjectTask

from {{ source("project_ref_models", "ProjectTaskRegisterItem") }} pti
join {{ source("project_ref_models", "ProjectTask") }} pt
on pt.TenantId = pt.TenantId
and pt.Id = pti.ProjectTaskId and pt.IsDeleted = 0
where pti.IsDeleted = 0