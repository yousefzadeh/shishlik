select distinct
ic.TenantId,
ic.IssueId RegisterItem_Id,
ic.ControlId RegisterItem_LinkedControlId,
c.Name RegisterItem_LinkedControl

from {{ source("register_ref_models", "IssueControlStatement") }} ic
join {{ source("controlset_ref_models", "Controls") }} c
on c.TenantId = ic.TenantId
and c.Id = ic.ControlId and c.IsDeleted = 0
where ic.IsDeleted = 0