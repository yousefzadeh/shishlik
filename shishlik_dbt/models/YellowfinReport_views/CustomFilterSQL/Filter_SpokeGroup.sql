{# 
DOC START
  - name: Filter_SpokeGroup
    description: |
        This view is used in a custom SQL filter for the following View:
        - Question Based Assessment Answer Details
               
        It lists One row per 
        - Spoke

        In The Filter Group "Assessment (ALL at Hub)", join with the filter on the tenantid:
        select 
        sg.*,
        t.Template_Name,
        t.Assessment_Name,
        t.ActualResults 
        from reporting.Filter_SpokeGroup sg
        join reporting.Filter_TemplateAssessmentActualResults t 
          on sg.Tenant_Id = t.Tenant_Id
        where sg.Tenant_Id = 3  

    columns:
      - name: Tenant_Id
        description: ID of Login Tenant for Access Filtering
      - name: Group_Level1
        description: Group Level 1 of the Spoke Ancestors
      - name: Group_Level2
        description: Group Level 2 of the Spoke Ancestors
      - name: Group_Level3Plus
        description: Group Level 3+ of the Spoke Ancestors
      - name: Spoke_Name
        description: Name of the Spoke (Tenant as Access Filter)

DOC END    
#}
with final as (
    select DISTINCT
      t.AbpTenants_Id as Tenant_Id,
      Rpt_VendorGroup_Level1Group Group_Level1, 
      Rpt_VendorGroup_Level2Group Group_Level2, 
      Rpt_VendorGroup_Level3PlusGroups Group_Level3Plus, 
      t.AbpTenants_Name Spoke_Name,
      MAX(GREATEST(t.AbpTenants_UpdateTime,vrvg.Rpt_VendorGroup_UpdateTime))OVER(PARTITION BY t.AbpTenants_Id,vrvg.Rpt_VendorGroup_Level1Group,vrvg.Rpt_VendorGroup_Level2Group, vrvg.Rpt_VendorGroup_Level3PlusGroups,t.AbpTenants_Name) Filter_UpdateTime
      
    FROM {{ ref("vwRpt_VendorGroup") }} vrvg 
    join {{ ref("vwRpt_TenantVendorGroup") }} vrtvg 
    on vrvg.Rpt_VendorGroup_VendorGroupId = vrtvg.Rpt_TenantVendorGroup_VendorGroupId 
    and vrvg.Rpt_VendorGroup_TenantId = vrtvg.Rpt_TenantVendorGroup_TenantId 
    join {{ ref("vwAbpTenants") }} t
    on vrtvg.Rpt_TenantVendorGroup_SpokeId = t.AbpTenants_Id 
    join {{ ref("vwTenantVendor") }} tv
    on t.AbpTenants_Id = tv.TenantVendor_VendorId
    where
    t.AbpTenants_IsActive = 1 and t.AbpTenants_IsDeleted =0 and tv.TenantVendor_IsArchived = 0
    -- Guard against tenants that are non hub spoke tenants
    and vrtvg.Rpt_TenantVendorGroup_TenantId in (
        select AbpTenants_Id 
        from {{ ref("vwAbpTenants") }} at2 
        where at2.AbpTenants_IsHubAndSpoke = 1
    )    
)
SELECT 
  Tenant_Id,
  Group_Level1,
  Group_Level2,
  Group_Level3Plus,
  Spoke_Name,
  Filter_UpdateTime
from final 
{# where Tenant_Id = 3 #}