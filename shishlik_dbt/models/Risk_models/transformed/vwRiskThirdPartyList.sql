-- RiskThirdParty_TenantVendor
-- One row per RiskId
-- Exclude the TenantId TenantVendor
select
    riskthirdparty_riskid,
    riskthirdparty_tenantid,
    left(
        string_agg(cast(tenantvendor_vendorid as nvarchar(max)), ', '), 4000
    ) vendoridlist,
    left(string_agg(cast(tenantvendor_name as nvarchar(max)), ', '), 4000) vendorlist
from {{ ref("vwRiskThirdPartyVendor") }}
group by riskthirdparty_riskid, riskthirdparty_tenantid
