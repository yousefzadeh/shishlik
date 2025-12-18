with final as (
    select 
    sg.Tenant_Id,
    sg.Group_Level1,
    sg.Group_Level2,
    sg.Group_Level3Plus,
    sg.Spoke_Name,
    i.Issues_Name,
    t.Tags_Name 
    from {{ ref("Filter_SpokeGroup") }} sg
    join {{ ref("vwIssues") }} i on sg.Tenant_Id = i.Issues_TenantId
    join {{ ref("vwIssueTag") }} it on i.Issues_Id = it.IssueTag_IssueId and it.IssueTag_TenantId = i.Issues_TenantId
    join {{ ref("vwTags") }} t on it.IssueTag_TagId = t.Tags_Id
    where i.Issues_Status != 100
)
select 
    Tenant_Id,
    Group_Level1,
    Group_Level2,
    Group_Level3Plus,
    Spoke_Name,
    Issues_Name,
    Tags_Name
from final
{# where Tenant_Id  in (select Spoke_TenantId from reporting.vwHubSpoke hs where Hub_TenantId = 1824) #}


