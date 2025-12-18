select
ip.TenantId,
ip.IssueId Issues_Id,
ip.AuthorityProvisionId Issues_LinkedProvisionId,
ap.Name Issues_LinkedProvision

from {{ source("issue_ref_models", "IssueProvision") }} ip
join {{ source("authority_ref_models", "AuthorityProvision") }} ap
on ap.Id = ip.AuthorityProvisionId and ap.IsDeleted = 0
where ip.IsDeleted = 0