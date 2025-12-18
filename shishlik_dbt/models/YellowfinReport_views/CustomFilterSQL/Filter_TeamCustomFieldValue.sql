{# 
DOC START
  - name: Filter_TeamCustomFieldValue
    description: |
        This view is used in a custom SQL filter for the following View:
        - Question Based Assessment Answer Details
               
        For QBA, in the set up for creation of assessments, Responding teams will be selected to respond to the assessment. 
        In the Third Party Management screen to setup the Responding Team (Vendor), the Custom Fields are defined with associated Values.
        For QBA, there are no Custom Fields defined for Assessments.

        It lists One row per 
        - Responding Team (Vendor)
        - Custom Field (For a given Responding Team)

    columns:
      - name: Tenant_Id
        description: ID of Login Tenant for Access Filtering
      - name: CustomField_Name
        description: Name of the Custom Field defined for the Tenant/Team 
      - name: CustomField_Value
        description: Value of the Custom Field defined for the Tenant/Team 

DOC END    
#}
with custom as (
    SELECT
        tv.TenantVendor_TenantId Tenant_Id,
        tv.TenantVendor_VendorId Vendor_Id,
        t.AbpTenants_Name Vendor_Name,
        ct.CustomFieldName CustomField_Name,
        ct.CustomFieldValue CustomField_Value,
        GREATEST(cast( tv.TenantVendor_UpdateTime as datetime2), cast(ct.ThirdPartyCustomTable_UpdateTime as datetime2), cast(t.AbpTenants_UpdateTime as datetime2)) AS custom_UpdateTime
    FROM
        {{ ref("vwThirdPartyCustomTable") }} ct
    INNER JOIN
        {{ ref("vwTenantVendor") }} tv
        ON ct.TenantVendor_Id = tv.TenantVendor_Id
    INNER JOIN {{ ref("vwAbpTenants") }} t
        ON t.AbpTenants_Id = tv.TenantVendor_VendorId
),
all_teams as ( -- For Parent Child Filters we need to return all rows of the parent Vendor/Team Name
    SELECT
      tv.TenantVendor_TenantId Tenant_Id,
      tv.TenantVendor_VendorId Vendor_Id,
      t.AbpTenants_Name Vendor_Name,
       GREATEST(cast(tv.TenantVendor_UpdateTime as datetime2), cast(t.AbpTenants_UpdateTime as datetime2)) AS all_teams_UpdateTime
    FROM
      {{ ref("vwAbpTenants") }} t
    join {{ ref("vwTenantVendor") }} tv
    on t.AbpTenants_Id = tv.TenantVendor_VendorId
    where
    t.AbpTenants_IsActive = 1 and t.AbpTenants_IsDeleted =0 and tv.TenantVendor_IsArchived = 0
),
final as (
  select distinct
  all_teams.Tenant_Id,
  all_teams.Vendor_Id,
  all_teams.Vendor_Name,
  custom.CustomField_Name,
  custom.CustomField_Value,
     max(GREATEST(cast(custom.custom_UpdateTime as datetime2), cast(all_teams.all_teams_UpdateTime as datetime2))) 
						over (partition by all_teams.Tenant_Id, all_teams.Vendor_Id, all_teams.Vendor_Name) AS Filter_UpdateTime
  from all_teams 
  left join custom on all_teams.Tenant_Id = custom.Tenant_Id and all_teams.Vendor_Id = custom.Vendor_Id
)
select 
Tenant_Id,
Vendor_Id,
-- Alias for Vendor_Name
Vendor_Name Spoke_Name,
Vendor_Name RespondingTeam_Name,
Vendor_Name,
CustomField_Name,
CustomField_Value,
Filter_UpdateTime
from final  
-- where Tenant_Id in (1384)
{# -- Hub 1824
where Tenant_Id in (1384,1825, 1829, 1830, 1831, 1832, 1833, 1838, 1861, 1920, 2036, 2041, 2048)  #}

