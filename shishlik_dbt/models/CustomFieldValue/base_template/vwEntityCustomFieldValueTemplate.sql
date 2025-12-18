
{# 
DOC START
  - name: vwEntityCustomFieldValueTemplate
    description: |
      This view forms a template on how to create a view for an Entity (Risk) with Custom Fields
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

    columns:
      - name: RoleType
        description: Whether the Custom Field is Created By or Accessed By the Tenant_Id
      - name: Tenant_Id
        description: |
            Login Tenant_Id 
            - One Tenant_Id - The User at Enterprise/Stand-alone Tenant or at Hub or at Spoke
            - Many Tenant_Id - The User at the hub is invited to view the data at the Spoke 
      - name: Entity_Id
        description: Entity Id - in this case is the Risk_Id
      - name: CustomField_Id
        description: FK to ThirdPartyControl table that defines the Custom Field
      - name: CustomField_Name
        description: Name of the Custom Field
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
      - name: CustomField_Value
        description: |
            Value assigned (Coverted to Text as per APP UI) to the Custom Field For the Entity (Risk_Id) at the Tenant (Tenant_Id)
            - Dropdown - One Value
            - Matrix - One Value
            - Multiselect Dropdown - Many Values
            - User - Many Values
            - Organization - Many Values
            - Free Text - One Value
            - Rich Text - One Value
            - Date - One Value formatted as "23 Aug, 2023"
            - Number - One Value formatted as "123456789", "0.123456789" (Not able to present as "123,456,789" using one format expression)
      - name: CustomField_ValueCode
        description: |
            For each Custom Field there is a list of Option Values defined in the vwCustomFieldOption view.
            For each Option Value there is a Value Code defined in the CustomFieldOption_ValueCode column.
            The selected Value (Displayed as Label in the UI) is stored in the CustomField_Value of this view.
            The related ValueCode (Displayed as Value in UI) is stored in this column.
      - name: CustomField_TextValue
        description: Text Value assigned to the Custom Field (Type Free Text, Rich Text)
      - name: CustomField_NumberValue
        description: Number Value assigned to the Custom Field (Type Number)
      - name: CustomField_DateValue
        description: Date Value assigned to the Custom Field (Type Date)
DOC END        
#}

with 
{#- 
    Entity to Custom Field Values assignments
    - Change table name for Entity to Custom Field Values assignments for selection from options  
	- Set value to NULL if feature is not present
    - Change table name for Entity to Custom Field Values assignments for user defined values
	- Set value to NULL if feature is not present
	Expose Entity_Id as <Entity>_Id at the final cte - Example is Risk_Id
	custom_field_defined CTE - change the EntityType
#}
entity_custom_selected as (
    select 
    RiskCustomAttributeData_TenantId Tenant_Id,
	RiskCustomAttributeData_RiskId Entity_Id, -- Risk_Id,
    RiskCustomAttributeData_ThirdPartyAttributesId SelectedOption_Id,
    RiskCustomAttributeData_ThirdPartyControlId CustomField_Id, 
    RiskCustomAttributeData_UserId UserId,
    RiskCustomAttributeData_OrganizationUnitId OrganizationUnitId
    from {{ ref("vwRiskCustomAttributeData") }}
),
entity_custom_user_defined as (
    select 
	RiskThirdPartyControlCustomText_TenantId Tenant_Id,
	RiskThirdPartyControlCustomText_RiskId Entity_Id, -- Risk_Id
    RiskThirdPartyControlCustomText_ThirdPartyControlId CustomField_Id,
    RiskThirdPartyControlCustomText_TextData TextValue,
    RiskThirdPartyControlCustomText_NumberValue NumberValue,
    RiskThirdPartyControlCustomText_CustomDateValue DateValue
    from {{ ref("vwRiskThirdPartyControlCustomText") }} 
),
{#- --------------------------------------------------------------------------------- #}
{#- 
    Custom Fields
    Custom Fields can be defined at Hub and then used at the Spokes under the rules set by each Tenant edition
#}
custom_field_defined as (
    {#- Custom Fields with Tenant it is Created at #}
	select 
	tpc.ThirdPartyControl_TenantId Tenant_Id,
	tpc.ThirdPartyControl_Id CustomField_Id, 
	tpc.ThirdPartyControl_Label CustomField_Name,
	tpc.ThirdPartyControl_Type CustomField_TypeId,
	tpc.ThirdPartyControl_TypeCode CustomField_Type,
	tpc.ThirdPartyControl_EntityTypeCode CustomFieldEntityType
	from {{ ref("vwThirdPartyControl") }} tpc
	where tpc.ThirdPartyControl_Enabled = 1
	and ThirdPartyControl_EntityType = 2 -- 'Risk'
),
custom_field_accessible as (
    {#- Custom Fields with Tenant it is Accessible from #}
	select 
	hs.Spoke_TenantId Tenant_Id,
	cfd.CustomField_Id, 
	cfd.CustomField_Name,
	cfd.CustomField_TypeId,
	cfd.CustomField_Type,
	cfd.CustomFieldEntityType
	from custom_field_defined cfd
	join {{ ref("vwHubSpoke") }} hs on cfd.Tenant_Id = hs.Hub_TenantId  
),
custom_field as (
    {#- Custom Fields Linked to Tenants it is Created at as well as Accessible from #}
	select custom_field_defined.*, 'Created By' RoleType from custom_field_defined 
	union all
	select custom_field_accessible.*, 'Accessed By' RoleType from custom_field_accessible
),
{#-
    Custom Field and All Possible Options for Type 1,4
#}
custom_field_options as (
	select 
	cf.Tenant_Id, 
	cf.CustomField_Id,
	cf.CustomField_Name,
	cf.CustomField_TypeId,
	cf.CustomField_Type,
	tpa.ThirdPartyAttributes_Id CustomFieldOption_Id, 
	tpa.ThirdPartyAttributes_Label CustomFieldOption_Value,
	tpa.ThirdPartyAttributes_Value CustomFieldOption_ValueCode
	from custom_field_defined cf
	join {{ ref("vwThirdPartyAttributes") }} tpa 
		on cf.CustomField_Id = tpa.ThirdPartyAttributes_ThirdPartyControlId 
),
{#- ---------------------------------------------------------------------------
    Custom Field Values
    Custom Field Values are defined at the Tenant (Tenant_Id) for the Entity (Risk_Id) for the Custom Field (CustomField_Id)
    - Grain: One row per Tenant per Entity per Custom Field per Custom Field Value Selected

    Select many Values from a list of Possible Options
    - 4 Multi-select dropdown 
#}
case_multi_dropdown as ( 
    {#- selected many out of many options presented #}
	select 
	cfs.Tenant_Id,
	cfs.Entity_Id,
	cfo.CustomField_Id,
	cfo.CustomField_Name,
	cfo.CustomFieldOption_Value CustomField_Value,
	cfo.CustomFieldOption_Value CustomField_TextValue,
	NULL CustomField_NumberValue,
	NULL CustomField_DateValue,
	cfo.CustomField_Type,
	cfo.CustomFieldOption_ValueCode CustomField_ValueCode
	from custom_field_options cfo          -- Custom Field Options
	inner join entity_custom_selected cfs -- Custom Field Selected 
		on cfs.SelectedOption_Id = cfo.CustomFieldOption_Id 
	where cfo.CustomField_TypeId = 4 -- 'Multiselect Dropdown'
),
{#- 
    Select One Value from a list of Possible Options 
    - 1 Dropdown
#}
case_dropdown as ( 
    {#- Selected one out of many options presented #}
	select 
	cfs.Tenant_Id,
	cfs.Entity_Id,
	cfo.CustomField_Id,
	cfo.CustomField_Name,
	cfo.CustomFieldOption_Value CustomField_Value,
	cfo.CustomFieldOption_Value CustomField_TextValue,
	NULL CustomField_NumberValue,
	NULL CustomField_DateValue,
	cfo.CustomField_Type,
	cfo.CustomFieldOption_ValueCode CustomField_ValueCode
	from custom_field_options cfo          -- Custom Field Options
	inner join entity_custom_selected cfs  -- Custom Field Selected 
		on cfs.SelectedOption_Id = cfo.CustomFieldOption_Id 
	where cfo.CustomField_TypeId in (1,2) -- ('Dropdown','Matrix')
),
{#-
    Many Users selected from List of Possible Users
    - 8 User - User Full Name 

    ThirdPartyControl > ThirdPartyAttributes.Type = 8 > RiskCustomAttributeData.UserId 
#}
case_user as ( 
	select 
	cfs.Tenant_Id,
	cfs.Entity_Id,
	cf.CustomField_Id,
	cf.CustomField_Name,
	u.AbpUsers_FullName CustomField_Value,
	u.AbpUsers_FullName CustomField_TextValue,
	NULL CustomField_NumberValue,
	NULL CustomField_DateValue,
	cf.CustomField_Type,
	NULL CustomField_ValueCode
	from custom_field cf                   -- Custom Field
	inner join entity_custom_selected cfs  -- Custom Field Selected 
		on cfs.CustomField_Id = cf.CustomField_Id 
        and cfs.Tenant_Id = cf.Tenant_Id 
	inner join {{ ref("vwAbpUser") }} u    
	    on u.AbpUsers_Id = cfs.UserId
	where cf.CustomField_TypeId = 8 --'User'
),
{#-
    Many Organisation Unit selected from List of Possible Organisation Units 
    - 8 User - Organisation Name 

    ThirdPartyControl > ThirdPartyAttributes.Type = 8 > RiskCustomAttributeData.OrganizationUnitId 

-#}
case_org as ( 
	select 
	cfs.Tenant_Id,
	cfs.Entity_Id,
	cf.CustomField_Id,
	cf.CustomField_Name,
	o.AbpOrganizationUnits_DisplayName CustomField_Value,
	o.AbpOrganizationUnits_DisplayName CustomField_TextValue,
	NULL CustomField_NumberValue,
	NULL CustomField_DateValue,
	cf.CustomField_Type,
	NULL CustomField_ValueCode
	from custom_field cf          -- Custom Field Options
	inner join entity_custom_selected cfs  -- Custom Field Selected 
		on cfs.CustomField_Id = cf.CustomField_Id 
        and cfs.Tenant_Id = cf.Tenant_Id 
	inner join {{ ref("vwAbpOrganizationUnits") }} o 
	    on o.AbpOrganizationUnits_Id = cfs.OrganizationUnitId
	where cf.CustomField_TypeId = 8 -- 'User'
),
{#- 
    User Defined Value - Text
    - 3 Free Text
    - 6 Rich Text
#}
case_text as (
	select 
	cfs.Tenant_Id,
	cfs.Entity_Id,
	cf.CustomField_Id,
	cf.CustomField_Name,
	cfs.TextValue CustomField_Value,
	cfs.TextValue CustomField_TextValue,
	NULL CustomField_NumberValue,
	NULL CustomField_DateValue,
	cf.CustomField_Type,
	NULL CustomField_ValueCode
	from custom_field cf          -- Custom Field Options
	inner join entity_custom_user_defined cfs  -- Custom Field Selected 
		on cfs.CustomField_Id = cf.CustomField_Id 
        and cfs.Tenant_Id = cf.Tenant_Id 
	where cf.CustomField_TypeId in (3,6) -- ('Free Text', 'Rich Text')
),
{#- 
    User Defined Value - Date
    - 5 Date
#}
case_date as (
	select 
	cfs.Tenant_Id,
	cfs.Entity_Id,
	cf.CustomField_Id,
	cf.CustomField_Name,
	FORMAT(cfs.DateValue, 'MMM d, yyyy') CustomField_Value,
	NULL CustomField_TextValue,
	NULL CustomField_NumberValue,
	cfs.DateValue CustomField_DateValue,
	cf.CustomField_Type,
	NULL CustomField_ValueCode
	from custom_field cf                       -- Custom Field Options
	inner join entity_custom_user_defined cfs  -- Custom Field Selected 
		on cfs.CustomField_Id = cf.CustomField_Id
        and cfs.Tenant_Id = cf.Tenant_Id 
	where cf.CustomField_TypeId = 5 -- 'Date'
),
{#- 
    User Defined Value - Number
    - 7 Number
#}
case_number as (
	select 
	cfs.Tenant_Id,
	cfs.Entity_Id,
	cf.CustomField_Id,
	cf.CustomField_Name,
	cast(cfs.NumberValue as varchar) CustomField_Value,
	NULL CustomField_TextValue,
	cfs.NumberValue CustomField_NumberValue,
	NULL CustomField_DateValue,
	cf.CustomField_Type,
	NULL CustomField_ValueCode
	from custom_field cf          -- Custom Field Options
	inner join entity_custom_user_defined cfs  -- Custom Field Selected 
		on cfs.CustomField_Id = cf.CustomField_Id 
        and cfs.Tenant_Id = cf.Tenant_Id 
	where cf.CustomField_TypeId = 7 -- 'Number'
),
{#-  
    All rows of the different case types
#}
case_union as (
	select * from case_dropdown
	union all 
	select * from case_multi_dropdown -- duplicate rows
	union all 
	select * from case_text
	union all 
	select * from case_date
	union all 
	select * from case_number
	union all 
	select * from case_user
	union all 
	select * from case_org
)
select
-- Grain at One row per Tenant per RiskId, per CustomField, per CustomFieldValue
cf.RoleType,
cf.Tenant_Id,
u.Entity_Id Risk_Id, -- Entity_Id is Risk_Id
cf.CustomField_Id,
cf.CustomField_Name,
cf.CustomField_Type,
u.CustomField_Value, -- Formatted Text of all values
-- The following columns retains the Data Type
u.CustomField_TextValue,
u.CustomField_NumberValue,
u.CustomField_DateValue,
u.CustomField_ValueCode
from custom_field cf
left join case_union u on cf.Tenant_Id = u.Tenant_Id and cf.CustomField_Id = u.CustomField_Id
