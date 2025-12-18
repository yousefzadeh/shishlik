with final as (
    select 
    sg.Tenant_Id,
    sg.Group_Level1,
    sg.Group_Level2,
    sg.Group_Level3Plus,
    sg.Spoke_Name,
    i.Issues_Name,
    ia.IssueAction_StatusCode,
    u.AbpUsers_FullName IssueAction_OwnerName,
    tv.TenantVendor_Name AssignedTeam_Name,
    ass.Assessment_Name
    from {{ ref("Filter_SpokeGroup") }} sg
    join {{ ref("vwIssues") }} i on sg.Tenant_Id = i.Issues_TenantId
    join {{ ref("vwIssueAction") }} ia on ia.IssueAction_IssueId = i.Issues_Id and ia.IssueAction_TenantId = i.Issues_TenantId
    left join {{ ref("vwAbpUser") }}u on ia.IssueAction_UserId = u.AbpUsers_Id and ia.IssueAction_TenantId = u.AbpUsers_TenantId
    left join {{ ref("vwTenantVendor")}} tv on tv.TenantVendor_id = ia.IssueAction_TenantVendorId and tv.TenantVendor_TenantId = ia.IssueAction_TenantId
    left join {{ ref("vwIssueAssessmentLink") }} ial on ial.IssueAssessment_IssueId = i.Issues_Id and ial.IssueAssessment_TenantId = i.Issues_TenantId
    left join {{ ref("vwAssessment") }} ass on ial.IssueAssessment_AssessmentId = ass.Assessment_Id 
    where i.Issues_Status != 100
)
select 
    Tenant_Id,
    Group_Level1,
    Group_Level2,
    Group_Level3Plus,
    Spoke_Name,
    Issues_Name,
    IssueAction_StatusCode,
    IssueAction_OwnerName,
    AssignedTeam_Name,
    Assessment_Name
from final
{# where Tenant_Id  in (select Spoke_TenantId from reporting.vwHubSpoke hs where Hub_TenantId = 1824) #}


