select distinct
pti.TenantId,
pti.RegisterItemId Asset_Id,
pt.ProjectId Asset_LinkedProjectId,
p.Name Asset_LinkedProject

from {{ source("project_ref_models", "ProjectTaskRegisterItem") }} pti
join {{ source("project_ref_models", "ProjectTask") }} pt
on pt.TenantId = pt.TenantId
and pt.Id = pti.ProjectTaskId and pt.IsDeleted = 0
join {{ source("project_ref_models", "Project") }} p
on p.TenantId = pt.TenantId
and p.Id = pt.ProjectId and p.IsDeleted = 0
where pti.IsDeleted = 0