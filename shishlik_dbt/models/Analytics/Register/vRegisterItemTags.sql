select
it.Uuid,
it.TenantId,
it.IssueId RegisterItem_Id,
it.TagId RegisterItem_TagId,
t.Name RegisterItem_Tag
from {{ source("register_ref_models", "IssueTag") }} it
join {{ source("miscellaneous_ref_models", "Tags") }} t
on t.Id = it.TagId and t.IsDeleted = 0
where it.IsDeleted = 0