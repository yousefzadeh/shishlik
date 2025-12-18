select distinct
pt.TenantId,
pt.ProjectId Project_Id,
pt.TagId Project_TagId,
t.Name Project_Tag
from {{ source("project_ref_models", "ProjectTag") }} pt
join {{ source("miscellaneous_ref_models", "Tags") }} t on t.Id = pt.TagId and t.IsDeleted = 0
where pt.IsDeleted = 0