select distinct
ip.TenantId,
ip.IssueId Issues_Id,
ap.AuthorityId Issues_LinkedAuthorityId,
a.Name Issues_LinkedAuthority

from {{ source("issue_ref_models", "IssueProvision") }} ip
join {{ source("authority_ref_models", "AuthorityProvision") }} ap
on ap.Id = ip.AuthorityProvisionId and ap.IsDeleted = 0
join {{ source("authority_ref_models", "Authority") }} a
on a.Id = ap.AuthorityId and a.IsDeleted = 0
where ip.IsDeleted = 0