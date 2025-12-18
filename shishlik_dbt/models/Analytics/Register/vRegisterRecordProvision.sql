select distinct
ip.TenantId,
ip.IssueId RegisterRecord_Id,
ip.AuthorityProvisionId AuthorityProvision_Id,
ap.Name RegisterItem_LinkedProvision

from {{ source("register_ref_models", "IssueProvision") }} ip
join {{ source("authority_ref_models", "AuthorityProvision") }} ap
on ap.Id = ip.AuthorityProvisionId and ap.IsDeleted = 0
where ip.IsDeleted = 0