select distinct
ic.TenantId,
ic.IssueId Issues_Id,
ic.ControlId Issues_LinkedControlId,
c.Name Issues_LinkedControl

from {{ source("issue_ref_models", "IssueControlStatement") }} ic
join {{ source("controlset_ref_models", "Controls") }} c
on c.TenantId = ic.TenantId
and c.Id = ic.ControlId and c.IsDeleted = 0
where ic.IsDeleted = 0