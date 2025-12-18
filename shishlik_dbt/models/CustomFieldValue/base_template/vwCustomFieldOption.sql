{# 
DOC START
  - name: vwCustomFieldOption
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
      - name: CustomFieldOption_Id
      - name: CustomFieldOption_Value
        description: |
          This is the String Value of the Custom Field Option that is displayed to the User in the App
      - name: CustomFieldOption_ValueCode
        description: |
          This is the Numeric Integer Value of the Custom Field Option that is displayed to the User in the App in the "Value" column of the drop down table.
DOC END
#}
with final as (
	select 
	cf.Tenant_Id, 
	cf.CustomField_Id,
	cf.CustomField_InternalDefaultName,
	cf.CustomField_Name,
	cf.CustomField_TypeId,
	cf.CustomField_Type,
	tpa.ThirdPartyAttributes_Id CustomFieldOption_Id, 
	tpa.ThirdPartyAttributes_Label CustomFieldOption_Value,
  tpa.ThirdPartyAttributes_Value CustomFieldOption_ValueCode
	from {{ ref("vwCustomFieldWithHubSpokeAccess")}} cf
	join {{ ref("vwThirdPartyAttributes") }} tpa 
		on cf.CustomField_Id = tpa.ThirdPartyAttributes_ThirdPartyControlId 
)
select
	Tenant_Id, 
	CustomField_Id,
	CustomField_InternalDefaultName,
	CustomField_Name,
	CustomField_TypeId,
	CustomField_Type,
	CustomFieldOption_Id, 
	CustomFieldOption_Value,
	CustomFieldOption_ValueCode
from final 
