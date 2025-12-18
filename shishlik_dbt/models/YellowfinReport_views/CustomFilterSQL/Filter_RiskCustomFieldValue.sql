{# 
DOC START
  - name: Filter_RiskCustomFieldValue
    description: |
      This view is used to populated filter in YF View
    columns:
      - name: Tenant_Id
        description: |
            Login Tenant_Id 
            - One Tenant_Id - The User at Enterprise/Stand-alone Tenant or at Hub or at Spoke
            - Many Tenant_Id - The User at the hub is invited to view the data at the Spoke 
      - name: CustomField_InternalDefaultName
        description: Internal Name of the Default Custom Field - RiskDomain or NULL (User defined)
      - name: CustomField_Name
        description: Name of the Custom Field for the Risk Entity
      - name: RiskCustomField_Value
        description: Value of the Custom Field for the Risk Entity
      - name: CustomField_Type
        description: Data type of the Custom Field for the Risk Entity
      - name: IsParentChild
        description: Flag to indicate if the Custom Field is a member of Parent Child Relationship
DOC END        
#}
with final as (
    select 
    r.Risk_TenantId Tenant_Id,
    r.Risk_Name,
    rcfv.CustomField_Name,
    rcfv.CustomField_Value RiskCustomField_Value,
    rcfv.CustomField_Type,
    rcfv.IsParentChild
    from {{ ref("vwRiskCustomFieldValue") }} rcfv
    join {{ ref("vwRisk") }} r on r.Risk_Id = rcfv.Risk_Id
    and r.Risk_TenantId = rcfv.Tenant_Id
)
select 
    Tenant_Id,
    Risk_Name,
    CustomField_Name,
    RiskCustomField_Value,
    CustomField_Type,
    IsParentChild
from final 