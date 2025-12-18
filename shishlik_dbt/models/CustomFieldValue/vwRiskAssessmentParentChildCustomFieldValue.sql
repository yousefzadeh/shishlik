{# 
DOC START
  - name: vwRiskAssessmentParentChildCustomFieldValue
    description: |
      This view shows Custom Field with or without Assigned Values with Parent Child Custom Fields up to 3 levels for Risk Assessments
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
      - name: CustomField_Level1Value
        description: Value of the Custom Field at Level 1
      - name: CustomField_Level2Id
        description: Id of the Custom Field at Level 2 
      - name: CustomField_Level2Name
        description: Name of the Custom Field at Level 2
      - name: CustomField_Level2Value
        description: Value of the Custom Field at Level 2
      - name: CustomField_Level3Id 
        description: Id of the Custom Field at Level 3
      - name: CustomField_Level3Name
        description: Name of the Custom Field at Level 3
      - name: CustomField_Level3Value
        description: Value of the Custom Field at Level 3
DOC END        
#}
{# 
  Risk Assessment assigned Custom Values to Custom Fields
#}
with entity_custom_value as (
  select 
    Tenant_Id,
    RiskAssessment_Id Entity_Id,
    CustomField_Id,
    CustomField_Value
  from {{ ref("vwRiskAssessmentCustomFieldValue")}}
),
final as (
  select 
  rcfv1.Tenant_Id,
  rcfv1.Entity_Id RiskAssessment_Id,
  pccf.CustomField_TypeId,
  pccf.CustomField_Type,
  pccf.CustomField_EntityType,
  pccf.CustomField_InternalDefaultName,
  pccf.CustomField_Level1Id, 
  pccf.CustomField_Level1Name,
  rcfv1.CustomField_Value CustomField_Level1Value,
  pccf.CustomField_Level2Id, 
  pccf.CustomField_Level2Name,
  rcfv2.CustomField_Value CustomField_Level2Value,
  pccf.CustomField_Level3Id, 
  pccf.CustomField_Level3Name,
  rcfv3.CustomField_Value CustomField_Level3Value
  from {{ ref("vwParentChildCustomField") }} pccf 
  left join entity_custom_value rcfv1
    on pccf.CustomField_Level1Id = rcfv1.CustomField_Id 
    and pccf.Tenant_Id = rcfv1.Tenant_Id
  left join entity_custom_value rcfv2
    on pccf.CustomField_Level2Id = rcfv2.CustomField_Id 
    and pccf.Tenant_Id = rcfv2.Tenant_Id
    and rcfv1.Entity_Id = rcfv2.Entity_Id
    and rcfv1.Tenant_Id = rcfv2.Tenant_Id
  left join entity_custom_value rcfv3
    on pccf.CustomField_Level3Id = rcfv3.CustomField_Id 
    and pccf.Tenant_Id = rcfv3.Tenant_Id
    and rcfv2.Entity_Id = rcfv3.Entity_Id
    and rcfv2.Tenant_Id = rcfv3.Tenant_Id
)
select 
  Tenant_Id,
  RiskAssessment_Id,
  CustomField_TypeId,
  CustomField_Type,
  CustomField_EntityType,
  CustomField_InternalDefaultName,
  CustomField_Level1Id, 
  CustomField_Level1Name,
  CustomField_Level1Value,
  CustomField_Level2Id, 
  CustomField_Level2Name,
  CustomField_Level2Value,
  CustomField_Level3Id, 
  CustomField_Level3Name,
  CustomField_Level3Value
from final 
{# where Tenant_Id = 1384 
and CustomField_InternalDefaultName = 'RiskDomain' #}
