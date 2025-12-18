{# 
DOC START
  - name: Filter_RiskSpokeGroupCustomFieldValue
    description: |
      This view is used to populated filter in YF View
    columns:
      - name: Tenant_Id
        description: |
            Login Tenant_Id 
            - One Tenant_Id - The User at Enterprise/Stand-alone Tenant or at Hub or at Spoke
            - Many Tenant_Id - The User at the hub is invited to view the data at the Spoke 
      - name: Group_Level1	
        description: Top Level of Spoke Group Parent Child Relationship
      - name: Group_Level2	
        description: Second Level of Spoke Group Parent Child Relationship
      - name: Group_Level3Plus	
        description: Third Level Plus of Spoke Group Parent Child Relationship
      - name: Spoke_Name
        description: Name of the Spoke Tenant in a Hub Spoke Relationship
      - name: CustomField_InternalDefaultName
        description: Internal Name of the Default Custom Field - RiskDomain or NULL (User defined)
      - name: RiskCustomField_Name
        description: Name of the Custom Field for the Risk Entity
      - name: CustomField_Value
        description: Value of the Custom Field for the Risk Entity
      - name: CustomField_Type
        description: Data type of the Custom Field for the Risk Entity
      - name: IsParentChild
        description: Flag to indicate if the Custom Field is a member of Parent Child Relationship

DOC END        
#}

with final as (
  select distinct 
  cfv.Tenant_Id,
  Group_Level1,	
  Group_Level2,	
  Group_Level3Plus,	
  Spoke_Name,
  cfv.CustomField_InternalDefaultName,
  cfv.CustomField_Name RiskCustomField_Name,
  cfv.CustomField_Value,
  cfv.CustomField_Type,
  cfv.IsParentChild
  from {{ ref("Filter_SpokeGroup") }} sg 
  join {{ ref("vwRiskCustomFieldValue") }} cfv on sg.Tenant_Id = cfv.Tenant_Id
)
select distinct
    Tenant_Id,
    Group_Level1,	
    Group_Level2,	
    Group_Level3Plus,	
    Spoke_Name,
    CustomField_InternalDefaultName,
    RiskCustomField_Name,
    CustomField_Value,
    CustomField_Type,
    IsParentChild
from final 
{# where Tenant_Id = 1838 -- Field and Value 110 rows, Value 90,   #}