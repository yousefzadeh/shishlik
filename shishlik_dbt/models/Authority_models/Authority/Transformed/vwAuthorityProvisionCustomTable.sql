{{ config(materialized="view") }}
{#- Authority_TenantId,
Authority_Id,
AuthorityProvision_Id,
AuthorityProvisionCustomField_FieldNameMax,
AuthorityProvisionCustomField_FieldName,
AuthorityProvisionCustomField_FieldNameWithUnassigned,
AuthorityProvisionCustomField_FieldNameValueMax,
AuthorityProvisionCustomField_FieldNameValue,
AuthorityProvisionCustomField_FieldNameValueWithUnassigned,
AuthorityProvisionCustomField_FieldType,
AuthorityProvisionCustomField_Order #}
select 
Tenant_Id Authority_TenantId,
AuthorityProvisionCustomValue_AuthorityId Authority_Id,
AuthorityProvisionCustomValue_AuthorityProvisionId AuthorityProvision_Id,
AuthorityProvisionCustomValue_FieldName AuthorityProvisionCustomField_FieldNameMax,
AuthorityProvisionCustomValue_FieldName AuthorityProvisionCustomField_FieldName,
case AuthorityProvisionCustomValue_FieldName 
when null then 'UnAssigned'
when '' then 'UnAssigned'
else AuthorityProvisionCustomValue_FieldName
end AuthorityProvisionCustomField_FieldNameWithUnassigned, 
AuthorityProvisionCustomValue_Value AuthorityProvisionCustomField_FieldNameValueMax,
AuthorityProvisionCustomValue_Value AuthorityProvisionCustomField_FieldNameValue,
case AuthorityProvisionCustomValue_Value 
when null then 'UnAssigned'
when '' then 'UnAssigned'
else AuthorityProvisionCustomValue_Value
end AuthorityProvisionCustomField_FieldNameValueWithUnassigned, 
AuthorityProvisionCustomValue_FieldType AuthorityProvisionCustomField_FieldType,
AuthorityProvisionCustomValue_FieldOrder AuthorityProvisionCustomField_Order
from{{ ref("vwAuthorityProvisionCustomValue") }}
{# where Tenant_Id = 1384 #}