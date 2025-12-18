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
        select distinct
            ap.AuthorityProvision_AuthorityId,
            ap.AuthorityProvision_Id,
            cast(ap.AuthorityProvision_ReferenceId as nvarchar(100)) AuthorityProvision_ReferenceId,
            cast(ap.AuthorityProvision_Name as nvarchar(4000)) AuthorityProvision_Name,
            ap.AuthorityProvision_CustomDataJson AuthorityProvision_CustomDataJson
        {# ,
	UpdateTime as AuthorityProvision_UpdateTime, -- uncomment when deploying tables -#}
        from {{ ref("vwAuthorityProvisionZero_source") }} ap
    ),
    apcustom as (  {# Provision with Custom Attributes and Values in rows -#}
        select
            AuthorityProvision_AuthorityId,
            AuthorityProvision_Id,
            AuthorityProvision_ReferenceId,
            AuthorityProvision_Name,
            {# Unnested JSON columns in rows -#}
            coalesce(cast(c. [key] as int) + 1, 0) as AuthorityProvisionCustom_ID,
            {# Internal key no reference to Authority Custom 
	cast(json_value(c.[value],'$.Id') as INT) as AuthorityProvisionCustom_ID, -#}
            cast(
                coalesce(json_value(c. [value], '$.Name'), 'Unassigned Field') as varchar(800)
            ) as AuthorityProvisionCustom_Field,
            case
                when json_value(c. [value], '$.Value') = ''
                then 'Blank'
                else cast(coalesce(json_value(c. [value], '$.Value'), 'Blank') as varchar(800))
            end as AuthorityProvisionCustom_Value
        {# ,
	AuthorityProvision_UpdateTime -#}
        from ap_raw OUTER APPLY OPENJSON(ap_raw.AuthorityProvision_CustomDataJson) as c
    ),
    apcustom_clean as (
        select
            AuthorityProvision_AuthorityId,
            coalesce(AuthorityProvision_Id, 0) AuthorityProvision_Id,
            AuthorityProvision_ReferenceId,
            AuthorityProvision_Name,
            coalesce(AuthorityProvisionCustom_ID, 0) AuthorityProvisionCustom_ID,
            AuthorityProvisionCustom_Field,
            replace(coalesce(AuthorityProvisionCustom_Value, 'Blank'), '<br>', '&nbsp;') AuthorityProvisionCustom_Value
        {# ,
	AuthorityProvision_UpdateTime -#}
        from apcustom
    ),
    auth_prov_custom_field_value as (  {# All Custom Attributes with Custom Values (If Any) #}
        select
            pcfv.AuthorityProvision_Id * 100 + AuthorityProvisionCustom_ID ProvisionCustom_Id,
            pcfv.AuthorityProvisionCustom_Field ProvisionCustom_FieldName,
            pcfv.AuthorityProvision_Id Provision_Id,
            {# Unnested JSON -#}
            pcfv.AuthorityProvisionCustom_ID ProvisionCustom_ValueId,
            pcfv.AuthorityProvisionCustom_Value ProvisionCustom_Value
        {# ,
	greatest(
		coalesce(pcfv.AuthorityProvision_UpdateTime,acf.AuthorityProvisionCustomField_UpdateTime),
		acf.AuthorityProvisionCustomField_UpdateTime
	) as ProvisionCustomFieldValue_UpdateTime -#}
        from apcustom_clean pcfv  {# Provision Custom Field Value -#}
    ),
    final as (
        select
            T.*,
            {# PK -#}
            'P'
            + cast(Provision_Id as varchar(10))
            + '@'
            + cast(ProvisionCustom_ID as varchar(5))
            + '='
            + cast(coalesce(ProvisionCustom_ValueId, 0) as varchar(5)) as ProvisionCustomFieldValue_PK
        from auth_prov_custom_field_value T
    )
select *
from final
