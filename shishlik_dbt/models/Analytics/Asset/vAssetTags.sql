select
it.Uuid,
it.TenantId,
it.IssueId Asset_Id,
it.TagId Asset_TagId,
t.Name Asset_Tag
from {{ source("asset_ref_models", "IssueTag") }} it
join {{ source("miscellaneous_ref_models", "Tags") }} t
on t.Id = it.TagId and t.IsDeleted = 0
where it.IsDeleted = 0