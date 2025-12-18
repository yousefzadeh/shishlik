select
    a.Authority_TenantId,
    a.Authority_Id,
    a.Authority_Name,
    act.AuthorityProvisionCustomField_FieldName,
    act.AuthorityProvisionCustomField_FieldNameValue,
    count(distinct ap.AuthorityProvision_Id) count_provision
from {{ ref("vwAuthority") }} a
join {{ ref("vwAuthorityProvision") }} ap on a.Authority_Id = ap.AuthorityProvision_AuthorityId
join
    {{ ref("vwAuthorityCustomTable") }} act
    on act.Authority_Id = a.Authority_Id
    and act.AuthorityProvision_Id = ap.AuthorityProvision_Id
{#- where a.Authority_TenantId = 3 #}
group by
    a.Authority_TenantId,
    a.Authority_Id,
    a.Authority_Name,
    act.AuthorityProvisionCustomField_FieldName,
    act.AuthorityProvisionCustomField_FieldNameValue
