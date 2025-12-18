{# 
DOC START
  - name: Filter_RiskSpokeGroupCustomField
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
      - name: IsParentChild
        description: Flag to indicate if the Custom Field is a member of Parent Child Relationship

DOC END        
#}

with final as (
    select
    cf.Tenant_Id,
    Group_Level1,	
    Group_Level2,	
    Group_Level3Plus,	
    Spoke_Name,
    cf.CustomField_InternalDefaultName,
    cf.CustomField_Name RiskCustomField_Name,
    cf.CustomField_Type,
    cf.IsParentChild
    from {{ ref("Filter_SpokeGroup") }} sg 
    join {{ ref("vwCustomFieldWithHubSpokeAccess") }} cf 
    on sg.Tenant_Id = cf.Tenant_Id
    where cf.CustomField_EntityTypeId = 2 
)
select 
    Tenant_Id,
    Group_Level1,	
    Group_Level2,	
    Group_Level3Plus,	
    Spoke_Name,
    CustomField_InternalDefaultName,
    RiskCustomField_Name,
    CustomField_Type,
    IsParentChild
from final 
{# where Tenant_Id = 1838 -- 42 rows #}