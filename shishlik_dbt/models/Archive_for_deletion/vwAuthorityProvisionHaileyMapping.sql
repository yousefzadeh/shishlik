-- Mapping of Direct Authority Provision to Hailey Authority Provision
select
    tapm.TenantAuthorityProvisionMapping_SourceAuthorityProvisionId Source_AuthorityProvisionId,
    tapm.TenantAuthorityProvisionMapping_TenantId mapping_TenantId,
    ap1.AuthorityProvision_TenantId Source_ap_TenantId,
    ap2.AuthorityProvision_TenantId Target_ap_TenantId,
    ap1.AuthorityProvision_AuthorityId Source_AuthorityId,
    ap1.AuthorityProvision_ReferenceId Source_ReferenceId,
    ap1.AuthorityProvision_Name Source_Name,
    tapm.TenantAuthorityProvisionMapping_TargetAuthorityProvisionId Target_AuthorityProvisionId,
    ap2.AuthorityProvision_AuthorityId Target_AuthorityId,
    ap2.AuthorityProvision_ReferenceId Target_ReferenceId,
    ap2.AuthorityProvision_Name Target_Name
from {{ ref("vwTenantAuthorityProvisionMapping") }} tapm
join
    {{ ref("vwAuthorityProvision") }} ap1
    on tapm.TenantAuthorityProvisionMapping_SourceAuthorityProvisionid = ap1.AuthorityProvision_Id
join
    {{ ref("vwAuthorityProvision") }} ap2
    on tapm.TenantAuthorityProvisionMapping_TargetAuthorityProvisionId = ap2.AuthorityProvision_Id
