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
        from {{ source("assessment_models", "AuthorityProvision") }} ap inner hash
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
            'Blank' as AuthorityProvisionCustom_Value
        from ap_raw
        where AuthorityProvision_CustomDataJson is NULL or AuthorityProvision_CustomDataJson = ''
    ),
    apcustom as (  {# Provision with Custom Attributes and Values in rows -#}
        select
            Tenant_Id,
            AuthorityProvision_AuthorityId,
            AuthorityProvision_Id,
            AuthorityProvision_ReferenceId,
            AuthorityProvision_Name,
            {# Unnested JSON columns in rows -#}
            c.Field_ID as AuthorityProvisionCustom_ID,
            {# Internal key no reference to Authority Custom 
	cast(json_value(c.[value],'$.Id') as INT) as AuthorityProvisionCustom_ID, -#}
            c.Field_Name as AuthorityProvisionCustom_Field,
            case when c.Field_Value = '' then 'Blank' else c.Field_Value end as AuthorityProvisionCustom_Value
        from ap_raw CROSS APPLY OPENJSON(ap_raw.AuthorityProvision_CustomDataJson)
        with
            (
                Field_ID Int '$.Id',
                Field_Name varchar(200) '$.Name',
                Field_Value varchar(4000) '$.Value',
                Field_Type varchar(100) '$.FieldType',
                Field_TypeId INT '$.FieldTypeId'
            ) as c
        where AuthorityProvision_CustomDataJson is not NULL and AuthorityProvision_CustomDataJson != ''
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
            pcfv.AuthorityProvisionCustom_Value ProvisionCustom_Value
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
            1 link_count
        from auth_prov_custom_field_value
    )
select *
from final
