{# 
DOC START
  - name: vwCustomFieldWithCreatedTenant
    description: |
      This view shows the Risk with Custom Fields and Values for the Tenant
    columns:
      - name: Tenant_Id
        description: |
          Tenant Id that can view and assign values to the Custom Field
          - Single Tenants
          - Hub Spoke Tenants
      - name: CustomField_Id 
      - name: CustomField_InternalDefaultName
        description: |
          This is the Internal Default Name of the Custom Field used in the App
      - name: CustomField_Name
        description: |
          This is the Name of the Custom Field that is displayed to the User in the App
      - name: CustomField_TypeId
      - name: CustomField_Type
        description: |
          This is the Type of the Custom Field
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
      - name: ParentCustomField_Id
        description: |
            This is the Parent Custom Field Id of the Custom Field
      - name: IsParentChild
        description: Flag to indicate if Custom Field is member of Parent Child Custom Field
DOC END
#}
with final as (
    {#- Custom Fields with Tenant it is Created at #}
	select 
	this.ThirdPartyControl_TenantId Tenant_Id,
	this.ThirdPartyControl_Id CustomField_Id, 
  this.ThirdPartyControl_Name CustomField_InternalDefaultName,
	this.ThirdPartyControl_Label CustomField_Name,
	this.ThirdPartyControl_Type CustomField_TypeId,
	this.ThirdPartyControl_TypeCode CustomField_Type,
	this.ThirdPartyControl_EntityType CustomField_EntityTypeId,
	this.ThirdPartyControl_EntityTypeCode CustomField_EntityType,
  this.ThirdPartyControl_ParentThirdPartyControlId ParentCustomField_Id,
  case
  when this.ThirdPartyControl_ParentThirdPartyControlId is null and child.ThirdPartyControl_Id is null then 0
  else 1
  end IsParentChild,
  'Created By' RoleType
	from {{ ref("vwThirdPartyControl") }} this 
  left join {{ ref("vwThirdPartyControl") }} child on this.ThirdPartyControl_Id = child.ThirdPartyControl_ParentThirdPartyControlId
  where this.ThirdPartyControl_Enabled = 1
)
select 
    Tenant_Id,
    CustomField_Id, 
    CustomField_InternalDefaultName,
    CustomField_Name,
    CustomField_TypeId,
    CustomField_Type,
    CustomField_EntityTypeId,
    CustomField_EntityType,
    ParentCustomField_Id,
    IsParentChild,
    RoleType
from final
