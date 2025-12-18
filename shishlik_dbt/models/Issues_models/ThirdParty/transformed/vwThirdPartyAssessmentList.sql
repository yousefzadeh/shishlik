with thirdparty_assessment as (
    select 
    TenantVendor_TenantId,
    TenantVendor_Id,
    Assessment_Name
    from {{ ref("vwTenantVendor") }} tv
    join {{ ref("vwAssessment") }} ass 
        on ass.Assessment_TenantVendorId = tv.TenantVendor_Id
        and ass.Assessment_TenantId = tv.TenantVendor_TenantId
),
final as (
    -- list of assessments per TenantVendor
    select 
    TenantVendor_TenantId Tenant_Id,
    TenantVendor_Id,
    string_agg(Assessment_Name,', ') Assessment_List
    from thirdparty_assessment
    group by 
    TenantVendor_TenantId,
    TenantVendor_Id
)
select
Tenant_Id,
TenantVendor_Id,
Assessment_List
from final 
{# where Tenant_Id = 1384 #}