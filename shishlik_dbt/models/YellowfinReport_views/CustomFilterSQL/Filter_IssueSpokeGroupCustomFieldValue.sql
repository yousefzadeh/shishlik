with final as (
    select 
    sg.Tenant_Id,
    sg.Group_Level1,
    sg.Group_Level2,
    sg.Group_Level3Plus,
    sg.Spoke_Name,
    i.Issues_Name,
    iou.AbpUsers_FullName Issues_OwnerName,
    i.Issues_PriorityCode,
    icfv.CustomField_Name,
    icfv.CustomField_Value
    from {{ ref("Filter_SpokeGroup") }} sg
    join {{ ref("vwIssues") }} i on sg.Tenant_Id = i.Issues_TenantId
    join {{ ref("vwIssueCustomFieldValue") }} icfv on icfv.Tenant_Id = i.Issues_TenantId and icfv.Issue_Id = i.Issues_Id
    join {{ ref("vwIssueOwner") }} io on i.Issues_Id = io.IssueOwner_IssueId and i.Issues_TenantId = io.IssueOwner_TenantId
    join {{ ref("vwAbpUser") }} iou on io.IssueOwner_UserId = iou.AbpUsers_Id
    where i.Issues_Status != 100
)
select 
    Tenant_Id,
    Group_Level1,
    Group_Level2,
    Group_Level3Plus,
    Spoke_Name,
    Issues_Name,
    Issues_OwnerName,
    Issues_PriorityCode,
    CustomField_Name,
    CustomField_Value
from final
{# where Tenant_Id  in (select Spoke_TenantId from reporting.vwHubSpoke hs where Hub_TenantId = 1824) #}