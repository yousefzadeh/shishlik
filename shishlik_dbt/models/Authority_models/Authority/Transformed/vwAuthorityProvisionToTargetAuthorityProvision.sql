{# 
    All Authority and its Provisions
       Target Authority ID if any 
       Target Provision ID if any
       Possible to have Target Authority ID without Target Provision ID 
#}
select 
mapping.TenantAuthorityProvisionMapping_TenantId Tenant_Id,
src_ap.AuthorityProvision_AuthorityId Authority_Id,            -- Source
src_ap.AuthorityProvision_Id,  -- Source
tgt_a.Authority_Id TargetAuthority_Id,
tgt_a.Authority_Name TargetAuthority_Name,
tgt_ap.AuthorityProvision_Id TargetAuthorityProvision_Id,
tgt_ap.AuthorityProvision_ReferenceId TargetAuthorityProvision_ReferenceId,
tgt_ap.AuthorityProvision_Name TargetAuthorityProvision_Name
from {{ ref("vwAuthorityProvision") }} src_ap 
join {{ ref("vwTenantAuthorityProvisionMapping") }} mapping -- source, target 
    on src_ap.Authorityprovision_Id = mapping.TenantAuthorityProvisionMapping_SourceAuthorityProvisionId
join {{ ref("vwAuthorityProvision") }} tgt_ap 
    on tgt_ap.AuthorityProvision_Id = mapping.TenantAuthorityProvisionMapping_TargetAuthorityProvisionId
join {{ ref("vwAuthority") }} tgt_a
    on tgt_a.Authority_Id = tgt_ap.AuthorityProvision_AuthorityId