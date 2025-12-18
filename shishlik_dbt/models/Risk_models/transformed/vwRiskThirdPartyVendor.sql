-- RiskThirdParty_TenantVendor
-- One row per RiskId
-- Exclude the TenantId TenantVendor
select
    rtp.riskthirdparty_riskid,
    rtp.riskthirdparty_tenantid,
    tv.tenantvendor_id,
    tv.tenantvendor_vendorid,
    tv.tenantvendor_name
from {{ ref("vwRiskThirdParty") }} rtp
join
    {{ ref("vwTenantVendor") }} tv
    on rtp.riskthirdparty_tenantvendorid = tv.tenantvendor_id
    and rtp.riskthirdparty_tenantid = tv.tenantvendor_tenantid
where rtp.riskthirdparty_tenantid != coalesce(tv.tenantvendor_vendorid, 0)
group by
    rtp.riskthirdparty_riskid,
    rtp.riskthirdparty_tenantid,
    tv.tenantvendor_id,
    tv.tenantvendor_vendorid,
    tv.tenantvendor_name
