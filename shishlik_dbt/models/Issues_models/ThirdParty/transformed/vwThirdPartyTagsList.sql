with thirdparty_tags as (
    SELECT DISTINCT 
    tp.TenantVendor_TenantId,
    tp.TenantVendor_Id,
    t.Tags_Name
    FROM {{ ref("vwTenantVendor") }} AS tp
    INNER JOIN {{ ref("vwThirdPartyTag") }} AS tpt
    ON tp.TenantVendor_Id = tpt.ThirdPartyTag_TenantVendorId
    AND tp.TenantVendor_TenantId = tpt.ThirdPartyTag_TenantId
    Inner JOIN {{ ref("vwTags") }} AS t
    ON tpt.ThirdPartyTag_TagId = t.Tags_ID
    AND tpt.ThirdPartyTag_TenantId = t.Tags_TenantId
),
final as (
    select 
    TenantVendor_TenantId Tenant_Id,
    TenantVendor_Id,
    string_agg(Tags_Name, ', ') as Tags_list
    from thirdparty_tags tpt 
    group by TenantVendor_TenantId, TenantVendor_Id
)
select 
Tenant_Id,
TenantVendor_Id,
Tags_list
from final
{# where Tenant_Id = 1384 #}