select distinct
ic.TenantId,
ic.IssueId Asset_Id,
pd.PolicyId Asset_LinkedControlSetId,
p.Name Asset_LinkedControlSet

from {{ source("asset_ref_models", "IssueControlStatement") }} ic
join {{ source("controlset_ref_models", "Controls") }} c
on c.TenantId = ic.TenantId
and c.Id = ic.ControlId and c.IsDeleted = 0
join {{ source("controlset_ref_models", "PolicyDomain") }} pd
on pd.TenantId = c.TenantId
and pd.Id = c.PolicyDomainId and pd.IsDeleted = 0
join {{ source("controlset_ref_models", "Policy") }} p
on p.TenantId = pd.TenantId
and p.Id = pd.PolicyId and p.IsDeleted = 0
where ic.IsDeleted = 0