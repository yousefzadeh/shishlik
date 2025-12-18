{{ config(materialized="view") }}
{# 
DOC START
  - name: vwThirdPartyControl
    description: |
      This view contains one row per Tenant per Entity Type per Custom Field that are enabled and not deleted.
    columns:
      - name: ThirdPartyControl_Id
      - name: ThirdPartyControl_Name
        description: |
          The Internal Default Name of the custom field.
      - name: ThirdPartyControl_LabelVarchar
      - name: ThirdPartyControl_Label
        description: |
          The Name of the custom field as displayed in APP UI.
      - name: ThirdPartyControl_Placeholder
        description: |
          The Instruction what to enter in the Label of the custom field as displayed in the UI.
      - name: ThirdPartyControl_Type
      - name: ThirdPartyControl_TypeCode
        description: |
          The Type of the custom field.
          1 - Dropdown
          2 - Dynamic/Matrix
          3 - Free Text
          4 - Multiselect Dropdown
          5 - Date
          6 - Rich Text
          7 - Number
          8 - User
      - name: ThirdPartyControl_Enabled
        description: |
          The Enabled flag of the custom field.
          1 - Enabled
          0 - Disabled
          In the case of Hierarchical custom fields, the parent custom field must be enabled for the child custom field to be enabled.  
          If the parent of an enabled child custom field is disabled then the APP will set the parent and the child custom field to be disabled.
      - name: ThirdPartyControl_EntityType
      - name: ThirdPartyControl_EntityTypeCode
        description: |
          The Entity Type of the custom field.
          0 - Third-Party
          1 - Asset
          2 - Risk
          3 - Risk Treatment
          4 - Risk Assessment
          5 - Register
          6 - Issue
          7 - Policy
          8 - Vulnerability
      - name: ThirdPartyControl_RegisterId
      - name: ThirdPartyControl_TenantId
        description: |
          The Tenant Id that created the custom field.
          In hub spoke environments, the Tenant Id will be the Id of the hub and spokes of the hub are allowed to view and assign values to the custom field.
      - name: ThirdPartyControl_ParentThirdPartyControlId
        description: |
          The Parent Id of the custom field.
          Custom fields may have parent child hierarchy up to 3 levels
      - name: ThirdPartyControl_Description
      - name: ThirdPartyControl_Order
        description: |
          The Order of the custom field as displayed in the UI.
DOC END    
#}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Name] as nvarchar(4000))[Name],
            cast([Label] as varchar(100))[Label],
            cast([Placeholder] as nvarchar(4000)) Placeholder,
            [Type],
            case Type
                when 1
                then 'Dropdown'
                when 2
                then 'Matrix'
                when 3
                then 'Free Text'
                when 4
                then 'Multiselect Dropdown'
                when 5
                then 'Date'
                when 6
                then 'Rich Text'
                when 7
                then 'Number'
                when 8
                then 'User'
                else 'Unknown'
            end
            [TypeCode],
            [Enabled],
            [TenantId],
            [EntityType],
            case EntityType
                when 0
                then 'Third-Party'
                when 1
                then 'Asset'
                when 2
                then 'Risk'
                when 3
                then 'Risk Treatment'
                when 4
                then 'Risk Assessment'
                when 5
                then 'Register'
                when 6
                then 'Issue'
                when 7
                then 'Policy'
                when 8
                then 'Vulnerability'
                else 'Unknown'
            end EntityTypeCode,
            [RegisterId],
            LabelVarchar,
            [Description],
            [Order],
            ParentThirdPartyControlId,
            Formula,
            cast(coalesce(LastModificationTime,CreationTime) as datetime2) as UpdateTime
        from {{ source("issue_models", "ThirdPartyControl") }} {{ system_remove_IsDeleted() }}
        and Enabled = 1
    )

select
    {{ col_rename("Id", "ThirdPartyControl") }},
    {{ col_rename("Name", "ThirdPartyControl") }},
    {{ col_rename("Label", "ThirdPartyControl") }},
    {{ col_rename("Placeholder", "ThirdPartyControl") }},

    {{ col_rename("Type", "ThirdPartyControl") }},
    {{ col_rename("TypeCode", "ThirdPartyControl") }},
    {{ col_rename("Enabled", "ThirdPartyControl") }},
    {{ col_rename("EntityType", "ThirdPartyControl") }},
    {{ col_rename("EntityTypeCode", "ThirdPartyControl") }},
    {{ col_rename("RegisterId", "ThirdPartyControl") }},
    {{ col_rename("TenantId", "ThirdPartyControl") }},
    {{ col_rename("LabelVarchar", "ThirdPartyControl") }},
    {{ col_rename("ParentThirdPartyControlId", "ThirdPartyControl") }},
    {{ col_rename("Description", "ThirdPartyControl") }},
    {{ col_rename("Order", "ThirdPartyControl") }},
    {{ col_rename("UpdateTime", "ThirdPartyControl") }}
from base
