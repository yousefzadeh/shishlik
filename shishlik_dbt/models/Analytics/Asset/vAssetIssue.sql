select
irr.TenantId,
irr.IssueId Asset_Id,
irr.LinkedIssueId Asset_LinkedIssueId,
i.Issues_Name Asset_LinkedIssue

from {{ source("asset_ref_models", "IssueRegisterRecord") }} irr
join {{ ref("vIssues") }} i
on i.Issues_Id = irr.LinkedIssueId
where irr.IsDeleted = 0