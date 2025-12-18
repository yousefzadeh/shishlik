with thirdparty_risk as (
    select 
    TenantVendor_TenantId,
    TenantVendor_Id,
    Risk_Name
    from {{ ref("vwTenantVendor") }} tv
    join {{ ref("vwRiskThirdParty") }} rtp
        on rtp.RiskThirdParty_TenantVendorId = tv.TenantVendor_Id
        and rtp.RiskThirdParty_TenantId = tv.TenantVendor_TenantId
    join {{ ref("vwRisk") }} r
        on r.Risk_Id = rtp.RiskThirdParty_RiskId
        and r.Risk_TenantId = rtp.RiskThirdParty_TenantId
),
final as (
    -- list of assessments per TenantVendor
    select 
    TenantVendor_TenantId Tenant_Id,
    TenantVendor_Id,
    string_agg(cast(Risk_Name as varchar(Max)),', ') Risk_List
    from thirdparty_risk
    group by 
    TenantVendor_TenantId,
    TenantVendor_Id
)
select
Tenant_Id,
TenantVendor_Id,
Risk_List
from final 
{# where Tenant_Id = 1384 #}