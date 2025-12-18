with thirdparty_issue as (
    select 
    TenantVendor_TenantId,
    TenantVendor_Id,
    Issues_Name
    from {{ ref("vwTenantVendor") }} tv
    join {{ ref("vwIssueThirdParty") }} itp
        on itp.IssueThirdParty_TenantVendorId = tv.TenantVendor_Id
        and itp.IssueThirdParty_TenantId = tv.TenantVendor_TenantId
    join {{ ref("vwIssues") }} i
        on i.Issues_Id = itp.IssueThirdParty_IssueId
        and i.Issues_TenantId = itp.IssueThirdParty_TenantId
),
final as (
    -- list of assessments per TenantVendor
    select 
    TenantVendor_TenantId Tenant_Id,
    TenantVendor_Id,
    string_agg(cast(Issues_Name as varchar(Max)),', ') Issue_List
    from thirdparty_issue
    group by 
    TenantVendor_TenantId,
    TenantVendor_Id
)
select
Tenant_Id,
TenantVendor_Id,
Issue_List
from final 
{# where Tenant_Id = 1384 #}