select distinct
ic.TenantId,
ic.IssueId Asset_Id,
ic.ControlId Asset_LinkedControlId,
c.Name Asset_LinkedControl

from {{ source("asset_ref_models", "IssueControlStatement") }} ic
join {{ source("controlset_ref_models", "Controls") }} c
on c.TenantId = ic.TenantId
and c.Id = ic.ControlId and c.IsDeleted = 0
where ic.IsDeleted = 0