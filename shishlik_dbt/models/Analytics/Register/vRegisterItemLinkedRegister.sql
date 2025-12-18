select distinct
irr.TenantId,
irr.IssueId RegisterItem_Id,
r.Register_Id RegisterItem_LinkedRegisterId,
r.Register_Name RegisterItem_LinkedRegister

from {{ source("register_ref_models", "IssueRegisterRecord") }} irr
join {{ ref("vRegisterRecord") }} rr
on rr.TenantId = irr.TenantId
and rr.Record_Id = irr.LinkedIssueId
join {{ ref("vRegister") }} r
on r.TenantId = rr.TenantId
and r.Register_Id = rr.Register_Id
where irr.IsDeleted = 0