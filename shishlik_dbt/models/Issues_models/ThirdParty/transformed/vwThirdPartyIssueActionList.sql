with thirdparty_issue_action as (
    select 
    TenantVendor_TenantId,
    TenantVendor_Id,
    Issues_Name,
    IssueAction_Title
    from {{ ref("vwTenantVendor") }} tv
    join {{ ref("vwIssueThirdParty") }} itp
        on itp.IssueThirdParty_TenantVendorId = tv.TenantVendor_Id
        and itp.IssueThirdParty_TenantId = tv.TenantVendor_TenantId
    join {{ ref("vwIssues") }} i
        on i.Issues_Id = itp.IssueThirdParty_IssueId
        and i.Issues_TenantId = itp.IssueThirdParty_TenantId
    join {{ ref("vwIssueAction") }} ia 
        on i.Issues_Id = ia.IssueAction_IssueId
        and i.Issues_TenantId = ia.IssueAction_TenantId
),
thirdparty_issue as (
    -- list of assessments per TenantVendor per Issues_Name
    select 
    TenantVendor_TenantId,
    TenantVendor_Id,
    Issues_Name,
    string_agg(cast(IssueAction_Title as varchar(Max)),', ') Action_List
    from thirdparty_issue_action
    group by 
    TenantVendor_TenantId,
    TenantVendor_Id,
    Issues_Name
),
final as (
    select 
    TenantVendor_TenantId Tenant_Id,
    TenantVendor_Id,
    string_agg(cast(('['+Issues_Name+': ' + Action_List +']') as varchar(max)), ', ') IssueAction_List
    from thirdparty_issue 
    group by 
    TenantVendor_TenantId,
    TenantVendor_Id
)
select
Tenant_Id,
TenantVendor_Id,
IssueAction_List
from final 
{# where Tenant_Id = 1384 #}