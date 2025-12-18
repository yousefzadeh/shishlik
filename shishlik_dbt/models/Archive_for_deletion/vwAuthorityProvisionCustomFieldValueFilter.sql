select
    ap.AuthorityProvision_TenantId,
    ap.AuthorityProvision_AuthorityId,
    ap.AuthorityProvision_Id,
    ap.AuthorityProvision_ReferenceId,
    ap.AuthorityProvision_Name,
    AuthorityProvisionCustomValue_FieldName as AuthorityProvision_CustomField,
    AuthorityProvisionCustomValue_Value as AuthorityProvision_CustomValue
from {{ ref("vwAuthorityProvisionCustomValue") }} apcv
join {{ ref("vwAuthorityProvision") }} ap on apcv.AuthorityProvisionCustomValue_AuthorityProvisionId = ap.AuthorityProvision_Id
