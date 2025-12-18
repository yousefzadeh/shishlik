select
irr.TenantId,
irr.IssueId Issues_Id,
irr.LinkedIssueId Issues_LinkedRegisterItemId,
r.Record_Name Issues_LinkedRegisterItem

from {{ source("issue_ref_models", "IssueRegisterRecord") }} irr
join {{ ref("vRegisterRecord") }} r
on r.Record_Id = irr.LinkedIssueId
where irr.IsDeleted = 0