select
it.Uuid,
it.TenantId,
it.IssueId Issues_Id,
it.TagId Issues_TagId,
t.Name Issues_Tag
from {{ source("issue_ref_models", "IssueTag") }} it
join {{ source("miscellaneous_ref_models", "Tags") }} t
on t.Id = it.TagId and t.IsDeleted = 0
where it.IsDeleted = 0