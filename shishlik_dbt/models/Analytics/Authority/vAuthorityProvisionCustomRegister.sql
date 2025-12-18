select
ip.TenantId,
ip.Id ProvisionRegister_Id,
ap.Authority_Id,
ip.AuthorityProvisionId AuthorityProvision_Id,
ip.IssueId RegisterRecord_Id,
rr.Record_Name Provision_linkedCustomRegisters

from {{ source("register_ref_models", "IssueProvision") }} ip
join {{ref("vAuthorityProvision")}} ap on ap.AuthorityProvision_Id = ip.AuthorityProvisionId
join {{ref("vRegisterRecord")}} rr on rr.Record_Id = ip.IssueId
join {{ref("vRegister")}} r on r.Register_Id = rr.Record_Id
where ip.IsDeleted = 0
and r.Register_EntityTypeCode = 4