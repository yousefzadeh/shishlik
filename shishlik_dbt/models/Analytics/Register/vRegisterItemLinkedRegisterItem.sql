select distinct
irr.TenantId,
irr.IssueId RegisterItem_Id,
irr.LinkedIssueId RegisterItem_LinkedRegisterItemId,
r.Record_Name RegisterItem_LinkedRegisterItem

from {{ source("register_ref_models", "IssueRegisterRecord") }} irr
join {{ ref("vRegisterRecord") }} r
on r.Record_Id = irr.LinkedIssueId
where irr.IsDeleted = 0