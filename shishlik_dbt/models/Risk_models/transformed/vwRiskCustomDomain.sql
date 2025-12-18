{# 
DOC START
  - name: vwRiskCustomDomain
    description: |
      This view shows Custom Field with or without Assigned Values with Parent Child Custom Fields up to 3 levels for Risks
    columns:
      - name: Risk_TenantId
        description: |
            Login Tenant_Id of a Risk
            - One Tenant_Id - The User at Enterprise/Stand-alone Tenant or at Hub or at Spoke
            - Many Tenant_Id - The User at the hub is invited to view the data at the Spoke 
      - name: Risk_Id
        description: |
            Id of Risk that has the assigned value to the Custom Field
      - name: Risk_Name
        description: |
            Name of Risk that has the assigned value to the Custom Field
      - name: CustomField_InternalDefaultName
        description: Default Name of the Custom Field used Internally and not editable by user.
      - name: CustomField_Level1Value
        description: Value of the Custom Field at Level 1
      - name: CustomField_Level2Value
        description: Value of the Custom Field at Level 2
      - name: CustomField_Level3Value
        description: Value of the Custom Field at Level 3
DOC END        
#}
with
    risk_parent_child as (
        select 
        rpccfv.Tenant_Id Risk_TenantId,
        rpccfv.Risk_Id,
        r.Risk_Name,
        CustomField_InternalDefaultName,
        CustomField_Level1Value Risk_Domain,
        CustomField_Level2Value Child_Domain,
        CustomField_Level3Value GrandChild_Domain
        from {{ ref("vwRiskParentChildCustomFieldValue") }} rpccfv 
        join {{ ref("vwRisk") }} r on r.Risk_Id = rpccfv.Risk_Id and r.Risk_TenantId = rpccfv.Tenant_Id
        where CustomField_InternalDefaultName = 'RiskDomain'
    )
select
    Risk_Id, 
    Risk_Name, 
    Risk_TenantId, 
    Risk_Domain, 
    Child_Domain, 
    GrandChild_Domain
from risk_parent_child 
{# where Risk_TenantId = 1384 #}
    
