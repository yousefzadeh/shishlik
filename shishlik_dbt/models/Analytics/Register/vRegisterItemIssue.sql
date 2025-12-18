--check
select
irr.TenantId,
irr.IssueId RegisterItem_Id,
irr.LinkedIssueId RegisterItem_LinkedIssueId,
i.Issues_Name RegisterItem_LinkedIssue

from {{ source("register_ref_models", "IssueRegisterRecord") }} irr
join {{ ref("vIssues") }} i
on i.Issues_Id = irr.LinkedIssueId
where irr.IsDeleted = 0