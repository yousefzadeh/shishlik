select
ip.TenantId,
ip.Id ProvisionIssue_Id,
ap.Authority_Id,
ip.AuthorityProvisionId AuthorityProvision_Id,
ip.IssueId Issue_Id,
i.Issues_Name Provision_linkedIssues

from {{ source("register_ref_models", "IssueProvision") }} ip
join {{ ref("vIssues") }} i on i.Issues_Id = ip.IssueId
join {{ ref("vAuthorityProvision") }} ap on ap.AuthorityProvision_Id = ip.AuthorityProvisionId
where ip.IsDeleted = 0