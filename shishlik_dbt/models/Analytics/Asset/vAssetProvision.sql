select distinct
ip.TenantId,
ip.IssueId Asset_Id,
ip.AuthorityProvisionId Asset_LinkedProvisionId,
ap.Name Asset_LinkedProvision

from {{ source("asset_ref_models", "IssueProvision") }} ip
join {{ source("authority_ref_models", "AuthorityProvision") }} ap
on ap.Id = ip.AuthorityProvisionId and ap.IsDeleted = 0
where ip.IsDeleted = 0