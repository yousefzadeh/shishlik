with thirdparty as (
    select 
    IssueAction_TenantId,
    IssueAction_Id,
    string_agg(TenantVendor_Name, ', ') ThirdPartyList
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
    group by 
    IssueAction_TenantId,
    IssueAction_Id
),
final as (
    select 
    IssueAction_TenantId Tenant_Id,
    IssueAction_Id,
    ThirdPartyList
    from thirdparty 
)
select 
Tenant_Id,
IssueAction_Id,
ThirdPartyList
from final
{# where Tenant_Id = 1384 #}