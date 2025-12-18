select 
pti.TenantId,
pti.RegisterItemId Issues_Id,
pti.ProjectTaskId Issues_LinkedProjectTaskId,
pt.Name Issues_LinkedProjectTask

from {{ source("project_ref_models", "ProjectTaskRegisterItem") }} pti
join {{ source("project_ref_models", "ProjectTask") }} pt
on pt.TenantId = pt.TenantId
and pt.Id = pti.ProjectTaskId and pt.IsDeleted = 0
where pti.IsDeleted = 0