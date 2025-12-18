{{ config(materialized="view") }}
{#- 
DOC START
  - name: vwAuthorityProvisionCustomValue
    description: |
      This view contains one row per Tenant per Authority per Provision per Custom Field
    columns:
      - name: Tenant_Id
        description: Login Tenant that has access to the Authority and Provision
      - name: AuthorityProvisionCustomValue_AuthorityId
      - name: AuthorityProvisionCustomValue_AuthorityProvisionId
      - name: AuthorityProvisionCustomValue_FieldId
        description: |
            FK to AutorityProvisionCustomField.  
            Covers these edge cases:
            - Custom field defined for the authority but no Provision has been assigned 
            - Custom field defined but no value assigned to the provision
            - Custom field defined value assigned is zero-length string
      - name: AuthorityProvisionCustomValue_FieldName
        description: |
            Name of the Custom Field defined for the Authority
      - name: AuthorityProvisionCustomValue_ValueId
        description: |
            Seq # of the 
      - name: AuthorityProvisionCustomValue_Value
      - name: AuthorityProvisionCustomValue_FieldType
      - name: AuthorityProvisionCustomValue_FieldOrder
DOC END    
#}
with
    direct_auth as (
        -- Group by - no duplicates
        select
            Tenant_Id,
            Authority_Id 
        from {{ ref("vwDirectAuthority") }}
    ), 
    assigned_base as (
{# 
    Optional Custom Fields for Authority are created at for Tenant in AuthorityProvisionCustomField
    Other tenants may refer to use the same Custom Field created by another Tenant.
    The vwDirectAuthority is used to Convert the Created TenantId into the TenantId that has access to the Custom Field.
    Custom field values for each AuthorityProvision is defined in CustomDataJson column in AuthorityProvision table.

    CustomDataJson field is of this format
        [
            {
                "Id": 557,
                "Name": "Title",
                "Value": " Background",
                "FieldType": "Singleline", This field is inconsistent, many nulls
                "FieldTypeId": 1           This field is also inconsistent, 0 when should be 1
            },
            {
                "Id": 558,
                "Name": "Description",
                "Value": "Lending secured by mortgages over residential property (residential mortgage lending) constitutes the largest credit exposure in the Australian banking system, and for many authorised deposit-taking institutions (ADIs) constitutes over half their total credit exposures. This concentration of exposure warrants ADIs paying particular attention to residential mortgage lending practices.",
                "FieldType": "Multiline",
                "FieldTypeId": 0
            },
            {
                "Id": 559,
                "Name": "Footnotes",
                "Value": "",
                "FieldType": "Multiline",
                "FieldTypeId": 0
            }
        ]

    Convert each JSON list to rows
    json list above will be converted to 3 rows with same AuthorityProvision_Id
    One row for each AuthorityProvision_Id

    Split out each JSON Entry into columns
    One row per AuthorityProvision per Custom Field
    FieldTypeId, FieldType are not consistently populated in the JSON by the APP.
    However we can take the FieldTypeId from AuthorityProvisionCustomField table.

    One row per AuthorityProvision per Custom Field of every row in AuthorityProvision table
    Every Authority has custom fields.
    Not every AuthorityProvision will have a custom value.
    Where no Custom value is assigned to a row of AuthorityProvision, Custom fields is shown with default value.
-#}
        select distinct
        a.Tenant_Id,
        a.Authority_Id,
        ap.Id AuthorityProvision_Id, 
        ap.Name AuthorityProvision_Name,
        ap.ReferenceId AuthorityProvision_ReferenceId,
        js.CustomField_Id,
        js.CustomField_Name,
        js.CustomValue_TypeId,
        js.CustomValue
        from direct_auth a 
        inner join {{ source("assessment_models", "AuthorityProvision") }} ap on ap.AuthorityId = a.Authority_Id and ap.IsDeleted = 0
        outer apply openjson(ap.CustomDataJson) WITH (  
                    CustomField_Id Int             '$.Id',
                    CustomField_Name Varchar(200)  '$.Name',
                    CustomValue_TypeId Int         '$.FieldTypeId',
                    CustomValue    nvarchar(max)    '$.Value'
                    ) as js
        -- where {# exception where these Tenants recorded the wrong field type -#}
        --     case 
        --     when ap.TenantId in (1,3,4,5,6,7,9,11,12,13,14,15,16,19,20,21,22,23,24,25,26) and js.CustomValue_TypeId = 0
        --     then 1
        --     else js.CustomValue_TypeId
        --     END = 1 -- Singleline
    ),
    assigned as(
	select distinct
        Tenant_Id,
        Authority_Id,
        AuthorityProvision_Id, 
        AuthorityProvision_Name,
        AuthorityProvision_ReferenceId,
        CustomField_Id,
        CustomField_Name,
        apcf.FieldType CustomValue_TypeId,
        CustomValue
		from assigned_base ab
		left join {{ source("assessment_models", "AuthorityProvisionCustomField") }} apcf on apcf.AuthorityId = ab.Authority_Id and apcf.Id = ab.CustomField_Id and apcf.IsDeleted = 0
		-- where apcf.FieldType = 1--commented to bring in all provisional custom fields
	),
    everything as (
        select
        a.Tenant_Id,
        a.Authority_Id,
        ap.Id AuthorityProvision_Id,
        ap.Name AuthorityProvision_Name,
        ap.ReferenceId AuthorityProvision_ReferenceId,
        apcf.Id CustomField_Id,
        apcf.FieldName,
        apcf.FieldType,
        NULL CustomValue
        from direct_auth a 
        inner join {{ source("assessment_models", "AuthorityProvision") }} ap on ap.AuthorityId = a.Authority_Id and ap.IsDeleted = 0
        inner join {{ source("assessment_models", "AuthorityProvisionCustomField") }} apcf on a.Authority_Id = apcf.AuthorityId
        -- where apcf.FieldType = 1--commented to bring in all provisional custom fields
    ),
    union_all as (
        select * from assigned
        union all 
        select * from everything
    ),
    final as (
        select
            Tenant_Id,
            Authority_Id AuthorityProvisionCustomValue_AuthorityId,
            AuthorityProvision_Id AuthorityProvisionCustomValue_AuthorityProvisionId,
            max(AuthorityProvision_Name) AuthorityProvisionCustomValue_AuthorityProvisionName,
            max(AuthorityProvision_ReferenceId) AuthorityProvisionCustomValue_AuthorityProvisionReferenceId,
            CustomField_Id AuthorityProvisionCustomValue_FieldId,
            max(CustomField_Name) AuthorityProvisionCustomValue_FieldName,
            0 AuthorityProvisionCustomValue_ValueId,
            max(CustomValue_TypeId) AuthorityProvisionCustomValue_FieldType,
            0 AuthorityProvisionCustomValue_FieldOrder,
            max(CustomValue) AuthorityProvisionCustomValue_Value
        from union_all
        group by
        Tenant_Id,
        Authority_Id,
        AuthorityProvision_Id,
        CustomField_Id,
        CustomField_Name
    )
select 
Tenant_Id,
AuthorityProvisionCustomValue_AuthorityId,
AuthorityProvisionCustomValue_AuthorityProvisionId,
AuthorityProvisionCustomValue_AuthorityProvisionName,
AuthorityProvisionCustomValue_AuthorityProvisionReferenceId,
AuthorityProvisionCustomValue_FieldId,
AuthorityProvisionCustomValue_FieldName,
AuthorityProvisionCustomValue_ValueId,
AuthorityProvisionCustomValue_Value,
AuthorityProvisionCustomValue_FieldType,
AuthorityProvisionCustomValue_FieldOrder
from final
{# where Tenant_Id = 1384 #}
