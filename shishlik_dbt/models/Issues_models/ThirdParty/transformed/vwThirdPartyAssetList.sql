with thirdparty_asset as (
    select 
    TenantVendor_TenantId,
    TenantVendor_Id,
    Asset_Title
    from {{ ref("vwTenantVendor") }} tv
    join {{ ref("vwIssueThirdParty") }} atv
        on atv.IssueThirdParty_TenantVendorId = tv.TenantVendor_Id
        and atv.IssueThirdParty_TenantId = tv.TenantVendor_TenantId
    join {{ ref("vwAsset") }} a
        on a.Asset_Id = atv.IssueThirdParty_IssueId
        and a.Asset_TenantId = atv.IssueThirdParty_TenantId
),
final as (
    -- list of assessments per TenantVendor
    select 
    TenantVendor_TenantId Tenant_Id,
    TenantVendor_Id,
    string_agg(Asset_Title,', ') Asset_List
    from thirdparty_asset
    group by 
    TenantVendor_TenantId,
    TenantVendor_Id
)
select
Tenant_Id,
TenantVendor_Id,
Asset_List
from final 