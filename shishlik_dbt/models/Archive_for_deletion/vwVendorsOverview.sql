select distinct
    t.AbpTenants_Id Tenant_Id,  -- access filter to %Tenant%
    att.Assessment_Name Filter_AssessmentTemplate_Name,  -- Filter on Template Name
    att.Assessment_IsArchived Filter_AssessmentTemplate_IsArchived,  -- Filter is Archived
    -- Table
    -- Level 1
    tv.TenantVendor_Name,
    tv.TenantVendor_Criticality,
    tv.TenantVendor_InherentRisk,
    tv.TenantVendor_Geography,
    tv.TenantVendor_Industry,
    -- Level 2
    e.Engagement_Id,
    e.Engagement_Name,
    e.Engagement_BusinessUnit,
    e.Engagement_Criticality,
    e.Engagement_InherentRisk,
    -- Level 3
    att.Assessment_ID AssessmentTemplate_Id,
    att.Assessment_Name + ' v' + cast(att.Assessment_TemplateVersion as varchar) AssessmentTemplate_NameVersion
from {{ ref("vwTenantVendor") }} tv
join {{ ref("vwAbpTenants") }} t on tv.TenantVendor_TenantId = t.AbpTenants_Id
join {{ ref("vwEngagement") }} e on tv.TenantVendor_Id = e.Engagement_TenantVendorId
join
    {{ ref("vwAssessment") }} a
    on a.Assessment_EngagementId = e.Engagement_Id
    and a.Assessment_WorkFlowId = 0
    and a.Assessment_TypeId = 1
join {{ ref("vwAssessment") }} att on a.Assessment_CreatedFromTemplateId = att.Assessment_ID
