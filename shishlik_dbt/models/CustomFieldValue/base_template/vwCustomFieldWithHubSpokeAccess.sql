{# 
DOC START
  - name: vwCustomFieldWithHubSpokeAccess
    description: |
      This view shows the Risk with Custom Fields and Values for the Tenant
    columns:
      - name: Tenant_Id
        description: |
          Tenant Id that can view and assign values to the Custom Field
          - Single Tenants
          - Hub Spoke Tenants
      - name: RoleType 
        description: |
          This is the Role Type of the Tenant
          - Created By
          - Accessed By
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
        description: |
            This is the flag to indicate if the Custom Field is a member of Parent Child Custom Field
DOC END
#}
with 
  tenant as (
    select 
    Id AbpTenants_Id,
    EditionId AbpTenants_EditionId
    from {{ source("assessment_models","AbpTenants") }}
    where IsDeleted = 0
  ),
  custom_field_created as (
    {#- Custom Fields with Tenant it is Created at 
        - Created at Hub 
        - Created at Spoke 
    -#}
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
      'Created By' RoleType
  from	{{ ref("vwCustomFieldWithCreatedTenant") }}
),
custom_field_standalone as (
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
  'Created By' RoleType
	from custom_field_created cf
	join tenant t 
    on cf.Tenant_Id = t.AbpTenants_Id -- Created at Tenant that is Stand-Alone
  join {{ ref("vwAbpEditions") }} e 
    on t.AbpTenants_EditionId = e.AbpEditions_Id 
  where e.AbpEditions_IsServiceProviderEdition = 0
),
custom_field_hub_spoke as (
    {#- Custom Fields with Tenant it is Accessible from 
        - Created at Hub - accessible at Hub and all spokes
        - Created at Spoke - accessible at the Spoke and also at Hub 
    -#}
	select 
	hs.Spoke_TenantId Tenant_Id,
	cfd.CustomField_Id, 
	cfd.CustomField_InternalDefaultName,
	cfd.CustomField_Name,
	cfd.CustomField_TypeId,
	cfd.CustomField_Type,
	cfd.CustomField_EntityTypeId,
	cfd.CustomField_EntityType,
  cfd.ParentCustomField_Id,
  cfd.IsParentChild,
  'Accessible By' RoleType
	from custom_field_created cfd
	join tenant t 
    on cfd.Tenant_Id = t.AbpTenants_Id -- Created at Tenant that is Stand-Alone
  join {{ ref("vwAbpEditions") }} e 
    on t.AbpTenants_EditionId = e.AbpEditions_Id 
	join {{ ref("vwHubSpoke") }} hs 
    on cfd.Tenant_Id = hs.Hub_TenantId -- Created at Hub 
  where e.AbpEditions_IsServiceProviderEdition = 1
  
  union all 

	select 
	hs.Spoke_TenantId Tenant_Id,
	cfd.CustomField_Id, 
	cfd.CustomField_InternalDefaultName,
	cfd.CustomField_Name,
	cfd.CustomField_TypeId,
	cfd.CustomField_Type,
	cfd.CustomField_EntityTypeId,
	cfd.CustomField_EntityType,
  cfd.ParentCustomField_Id,
  cfd.IsParentChild,
  'Accessible By' RoleType
	from custom_field_created cfd
	join tenant t 
    on cfd.Tenant_Id = t.AbpTenants_Id -- Created at Tenant that is Stand-Alone
  join {{ ref("vwAbpEditions") }} e 
    on t.AbpTenants_EditionId = e.AbpEditions_Id 
	join {{ ref("vwHubSpoke") }} hs 
    on cfd.Tenant_Id = hs.Spoke_TenantId -- Created at Spoke
  where e.AbpEditions_IsServiceProviderEdition = 0
),
custom_field as (
    {#- Custom Fields Linked to Tenants it is Created at as well as Accessible from #}
	select * from custom_field_standalone
	union all
	select * from custom_field_hub_spoke
),
final as (
  {#- 
      Remove duplicates in case of Created at Spoke and Accessible at Spoke
   -#}
  select 
    Tenant_Id,
    CustomField_Id, 
    max(CustomField_InternalDefaultName) CustomField_InternalDefaultName,
    max(CustomField_Name) CustomField_Name,
    max(CustomField_TypeId) CustomField_TypeId,
    max(CustomField_Type) CustomField_Type,
    max(CustomField_EntityTypeId) CustomField_EntityTypeId,
    max(CustomField_EntityType) CustomField_EntityType,
    max(ParentCustomField_Id) ParentCustomField_Id,
    max(IsParentChild) IsParentChild,
    max(RoleType) RoleType -- Chose Created By over Accessed By
  from custom_field
  group by Tenant_Id, CustomField_Id 
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
