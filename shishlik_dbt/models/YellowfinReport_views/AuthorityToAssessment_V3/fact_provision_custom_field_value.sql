{{- config(materialized="view") -}}
{#-
	This view is used to retrieve the complete structure of Custom attributes in Authority and Provisions.
	1. Custom Attributes are defined for each Authority 
	2. Each Authority has many Provisions but each Provision is related to only 1 Authority
	3. Custom Values of each Custom Attribute are defined for each Provision in a JSON column "CustomDataJson"
	4. One row in this table is for 1 Authority, 1 Provision, 1 Custom Attribute and 1 Custom Value
	5. If there are no custom values then One row for 1 Authority, 1 Provision, 1 Custom Attribute and Custom Value as 'Blank'

    Add zero keys for NULL 

	Reference: https://dev.azure.com/admin0011/6clicks/_wiki/wikis/6clicks.wiki/250/Authorities?anchor=provision-custom-attributes
-#}
with
    ap_raw as (  {# Provision table with Custom Attribute and Values in CustomJson #}
        select
            ap.TenantId Tenant_Id,
            ap.AuthorityId AuthorityProvision_AuthorityId,
            ap.Id AuthorityProvision_Id,
            ap.ReferenceId AuthorityProvision_ReferenceId,
            ap.Name AuthorityProvision_Name,
            ap.CustomDataJson AuthorityProvision_CustomDataJson
        from {{ source("assessment_models", "AuthorityProvision") }} ap
        where ap.IsDeleted = 0
        union
        select
            ta.TenantId Tenant_Id,
            ap.AuthorityId AuthorityProvision_AuthorityId,
            ap.Id AuthorityProvision_Id,
            ap.ReferenceId AuthorityProvision_ReferenceId,
            ap.Name AuthorityProvision_Name,
            ap.CustomDataJson AuthorityProvision_CustomDataJson
        from {{ source("assessment_models", "AuthorityProvision") }} ap
        join {{ source("tenant_models", "TenantAuthority") }} ta on ap.AuthorityId = ta.AuthorityId
        where ap.IsDeleted = 0 and ta.IsDeleted = 0
    ),
    apcustom_zero as (
        select
            Tenant_Id,
            AuthorityProvision_AuthorityId,
            AuthorityProvision_Id,
            AuthorityProvision_ReferenceId,
            AuthorityProvision_Name,
            0 as AuthorityProvisionCustom_ID,
            'Unassigned Field' as AuthorityProvisionCustom_Field,
            'Blank' as AuthorityProvisionCustom_Value,
            1 as AuthorityProvisionCustom_Type
        from ap_raw
        where AuthorityProvision_CustomDataJson is NULL or AuthorityProvision_CustomDataJson = ''
    ),
    apcustom as (
        select 
        Tenant_Id,
        AuthorityProvisionCustomValue_AuthorityId AuthorityProvision_AuthorityId,
        AuthorityProvisionCustomValue_AuthorityProvisionId AuthorityProvision_Id,
        AuthorityProvisionCustomValue_AuthorityProvisionReferenceId AuthorityProvision_ReferenceId,
        AuthorityProvisionCustomValue_AuthorityProvisionName AuthorityProvision_Name,
        AuthorityProvisionCustomValue_FieldId AuthorityProvisionCustom_ID,
        AuthorityProvisionCustomValue_FieldName AuthorityProvisionCustom_Field,
        case 
        when AuthorityProvisionCustomValue_Value = '' 
        then 'Blank' 
        else AuthorityProvisionCustomValue_Value 
        end  AuthorityProvisionCustom_Value,
        AuthorityProvisionCustomValue_FieldType AuthorityProvisionCustom_Type
        from {{ ref("vwAuthorityProvisionCustomValue") }}
    ),
    apcustom_union as (
        select *
        from apcustom
        union all
        select *
        from apcustom_zero
    ),
    auth_prov_custom_field_value as (  {# All Custom Attributes with Custom Values (If Any) #}
        select
            pcfv.Tenant_Id,
            pcfv.AuthorityProvision_Id * 10000 + AuthorityProvisionCustom_ID ProvisionCustom_Id,
            pcfv.AuthorityProvisionCustom_Field ProvisionCustom_FieldName,
            pcfv.AuthorityProvision_Id Provision_Id,
            pcfv.AuthorityProvisionCustom_ID ProvisionCustom_ValueId,
            pcfv.AuthorityProvisionCustom_Value ProvisionCustom_Value,
            pcfv.AuthorityProvisionCustom_Type ProvisionCustom_Type
        from apcustom_union pcfv  {# Provision Custom Field Value -#}
    ),
    final as (
        select
            Tenant_Id,
            ProvisionCustom_Id,
            Provision_Id,
            ProvisionCustom_ValueId,
            ProvisionCustom_FieldName,
            ProvisionCustom_Value,
            ProvisionCustom_Type,
            1 link_count
        from auth_prov_custom_field_value
    )
select *
from final
{# where Tenant_Id = 1384 #}