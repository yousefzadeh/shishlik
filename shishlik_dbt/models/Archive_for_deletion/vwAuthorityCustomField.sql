{{ config(materialized="view") }}

select distinct
    Authority_Id,
    Authority_TenantId,
    CAST(AuthorityProvisionCustomField_FieldName as VARCHAR(100)) as AuthorityProvisionCustomField_FieldNameMax,
    CAST(AuthorityProvisionCustomField_FieldName as VARCHAR(100)) as AuthorityProvisionCustomField_FieldName,
    AuthorityProvisionCustomField_FieldType,
    AuthorityProvisionCustomField_Order
from {{ ref("vwAuthorityProvisionCustomTable") }} q
