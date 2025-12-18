select distinct
irr.TenantId,
irr.IssueId Asset_Id,
r.Register_Id Asset_LinkedRegisterId,
r.Register_Name Asset_LinkedRegister

from {{ source("asset_ref_models", "IssueRegisterRecord") }} irr
join {{ ref("vRegisterRecord") }} rr
on rr.TenantId = irr.TenantId
and rr.Record_Id = irr.LinkedIssueId
join {{ ref("vRegister") }} r
on r.TenantId = rr.TenantId
and r.Register_Id = rr.Register_Id
where irr.IsDeleted = 0