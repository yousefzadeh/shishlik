{# 
DOC START
  - name: vwParentChildCustomField
    description: |
      This view forms a template on how to create a view for an Entity (Risk) with Hierarchical Custom Fields up to 3 levels
      Fixes Issues identified:
        1. Spoke has access to Custom Field defined in Hub
        - Volume of rows explodes
        2. Non-text Custom Values and its representation
        - DateValue
        - User/Organization 
        - Number
        - Rich Text - HTML Text
        - Formatting in Yellowfin columns
        3. Grain for Multi-Select Dropdown (Type 3)
        - Filter - multi-row
        - Report column - single row, comma separated list
        4. All custom fields to be listed with or without values defined
        5. Performance
        6. Limited to 3 levels

    columns:
      - name: Tenant_Id
        description: |
            Login Tenant_Id 
            - One Tenant_Id - The User at Enterprise/Stand-alone Tenant or at Hub or at Spoke
            - Many Tenant_Id - The User at the hub is invited to view the data at the Spoke 
      - name: CustomField_TypeId
      - name: CustomField_Type
        description: |
            Type of the Custom Field
            - 1 Dropdown
            - 2 Matrix
            - 3 Free Text
            - 4 Multiselect Dropdown
            - 5 Date
            - 6 Rich Text
            - 7 Number
            - 8 User (User or Organization Unit Multiselect)
      - name: CustomField_EntityType
        description: |
          Entity of the Custom Field
          - 0 'Third-Party'
          - 1 'Asset'
          - 2 'Risk'
          - 3 'Risk Treatment'
          - 4 'Risk Assessment'
          - 5 'Register'
          - 6 'Issue'
          - 7 'Policy'
          - 8 'Vulnerability'
      - name: CustomField_InternalDefaultName
        description: Default Name of the Custom Field used Internally and not editable by user. 
      - name: CustomField_Level1Id
        description: Id of the Custom Field at Level 1
      - name: CustomField_Level1Name
        description: Name of the Custom Field at Level 1
      - name: CustomField_Level2Id
        description: Id of the Custom Field at Level 2 
      - name: CustomField_Level2Name
        description: Name of the Custom Field at Level 2
      - name: CustomField_Level3Id 
        description: Id of the Custom Field at Level 3
      - name: CustomField_Level3Name
        description: Name of the Custom Field at Level 3
DOC END        
#}

with 
{#- --------------------------------------------------------------------------------- #}
{#- 
    Custom Fields
    Custom Fields can be defined at Hub and then used at the Spokes under the rules set by each Tenant edition
#}
custom_field as (
    {#- Custom Fields with Tenant it is Created at #}
  select 
    Tenant_Id,
    CustomField_Id, 
    CustomField_InternalDefaultName,
    CustomField_Name,
    CustomField_TypeId,
    CustomField_Type,
    CustomField_EntityType,
    ParentCustomField_Id,
    RoleType
  from {{ ref("vwCustomFieldWithCreatedTenant") }}
),
final as (
  select 
	level1.Tenant_Id,
	level1.CustomField_TypeId,
	level1.CustomField_Type,
	level1.CustomField_EntityType,
	level1.CustomField_InternalDefaultName,
	level1.CustomField_Id CustomField_Level1Id, 
	level2.CustomField_Id CustomField_Level2Id, 
	level3.CustomField_Id CustomField_Level3Id, 
	level1.CustomField_Name CustomField_Level1Name,
	level2.CustomField_Name CustomField_Level2Name,
	level3.CustomField_Name CustomField_Level3Name
  from custom_field level1
  left join custom_field level2 on level2.ParentCustomField_Id = level1.CustomField_Id and level2.Tenant_Id = level1.Tenant_Id
  left join custom_field level3 on level3.ParentCustomField_Id = level2.CustomField_Id and level3.Tenant_Id = level2.Tenant_Id
  where level1.ParentCustomField_Id is null
)
select * 
from final
