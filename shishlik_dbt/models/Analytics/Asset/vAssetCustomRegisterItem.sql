select distinct
irr.TenantId,
irr.IssueId Asset_Id,
irr.LinkedIssueId Asset_LinkedRegisterItemId,
r.Record_Name Asset_LinkedRegisterItem

from {{ source("asset_ref_models", "IssueRegisterRecord") }} irr
join {{ ref("vRegisterRecord") }} r
on r.Record_Id = irr.LinkedIssueId
where irr.IsDeleted = 0