select distinct
ic.TenantId,
ic.IssueId Issues_Id,
ic.StatementId Issues_LinkedControlResponsibilityId,
s.Title Issues_LinkedControlResponsibility

from {{ source("issue_ref_models", "IssueControlStatement") }} ic
join {{ source("controlset_ref_models", "Statement") }} s
on s.TenantId = ic.TenantId
and s.Id = ic.StatementId and s.IsDeleted = 0
where ic.IsDeleted = 0