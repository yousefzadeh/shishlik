select distinct
ic.TenantId,
ic.IssueId Issues_Id,
pd.PolicyId Issues_LinkedControlSetId,
p.Name Issues_LinkedControlSet

from {{ source("issue_ref_models", "IssueControlStatement") }} ic
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