{# 
DOC START
  - name: vwRiskChildCustomFieldValue
    description: |
      This view shows Custom Field with Assigned Values with Parent Child Custom Fields of Level 2 for Risk
    columns:
      - name: Tenant_Id
        description: |
            Login Tenant_Id 
            - One Tenant_Id - The User at Enterprise/Stand-alone Tenant or at Hub or at Spoke
            - Many Tenant_Id - The User at the hub is invited to view the data at the Spoke 
      - name: RiskAssessment_Id
        description: |
            Id of the Risk Assessment that has assigned value to the custom field
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
      - name: CustomField_InternalDefaultName
        description: Default Name of the Custom Field used Internally and not editable by user.
      - name: CustomField_Level2Value
        description: Value of the Custom Field at Level 2
DOC END        
#}
{# 
  Risk Assessment assigned Custom Values to Custom Fields
#}
with entity_custom_value as (
  select 
    Tenant_Id,
    Risk_Id Entity_Id,
    CustomField_Id,
    CustomField_Value
  from {{ ref("vwRiskCustomFieldValue")}}
),
custom_field as (
    {#- Custom Fields with Tenant it is Created at #}
  select 
    Tenant_Id,
    CustomField_Id, 
    CustomField_InternalDefaultName,
    CustomField_Name,
    CustomField_TypeId,
    CustomField_Type,
    ParentCustomField_Id,
    RoleType
  from {{ ref("vwCustomFieldWithHubSpokeAccess") }}
  where CustomField_EntityTypeId = 2 -- Risk
),
custom_field_level2 as (
  select 
	level1.Tenant_Id,
	level1.CustomField_TypeId,
	level1.CustomField_Type,
	level1.CustomField_InternalDefaultName,
	level2.CustomField_Id CustomField_Level2Id
  from custom_field level1
  join custom_field level2 on level2.ParentCustomField_Id = level1.CustomField_Id and level2.Tenant_Id = level1.Tenant_Id
  where level1.ParentCustomField_Id is null
),
final as (
  select 
  value2.Tenant_Id,
  value2.Entity_Id Risk_Id,
  field2.CustomField_TypeId,
  field2.CustomField_Type,
  field2.CustomField_InternalDefaultName,
  value2.CustomField_Value CustomField_Level2Value
  from custom_field_level2 field2 
  join entity_custom_value value2
    on field2.CustomField_Level2Id = value2.CustomField_Id 
    and field2.Tenant_Id = value2.Tenant_Id
)
select 
  Tenant_Id,
  Risk_Id,
  CustomField_TypeId,
  CustomField_Type,
  CustomField_InternalDefaultName,
  CustomField_Level2Value
from final 
{# where Tenant_Id = 1384 
and CustomField_InternalDefaultName = 'RiskDomain' #}
