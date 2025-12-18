select distinct
ic.TenantId,
ic.IssueId RegisterItem_Id,
ic.StatementId RegisterItem_LinkedControlResponsibilityId,
s.Title RegisterItem_LinkedControlResponsibility

from {{ source("register_ref_models", "IssueControlStatement") }} ic
join {{ source("controlset_ref_models", "Statement") }} s
on s.TenantId = ic.TenantId
and s.Id = ic.StatementId and s.IsDeleted = 0
where ic.IsDeleted = 0