select distinct
ic.TenantId,
ic.IssueId Asset_Id,
ic.StatementId Asset_LinkedControlResponsibilityId,
s.Title Asset_LinkedControlResponsibility

from {{ source("asset_ref_models", "IssueControlStatement") }} ic
join {{ source("controlset_ref_models", "Statement") }} s
on s.TenantId = ic.TenantId
and s.Id = ic.StatementId and s.IsDeleted = 0
where ic.IsDeleted = 0