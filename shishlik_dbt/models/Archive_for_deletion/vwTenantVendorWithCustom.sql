-- ThirdPartyCustomTable aggregated to one row of tenantvendor_Id
with
    tv as (select tv.* from {{ ref("vwTenantVendor") }} tv),
    cv as (
        -- CustomValue per CustomFieldName per TenantVendor_Id
        select tpct.TenantVendor_Id, tpct.CustomFieldName, string_agg(tpct.CustomFieldValue, ', ') ListOfCustomValues
        from {{ ref("vwThirdPartyCustomTable") }} tpct
        group by tpct.TenantVendor_Id, tpct.CustomFieldName
    ),
    cv2 as (
        -- One row Custom fields and values per TenantVendor_Id
        select cv.TenantVendor_Id, string_agg('[' + CustomFieldName + ' = ' + ListOfCustomValues + ']', ', ') Custom
        from cv
        group by cv.TenantVendor_Id
    )
select tv.*, Custom
from cv2
join tv on tv.TenantVendor_Id = cv2.TenantVendor_Id
