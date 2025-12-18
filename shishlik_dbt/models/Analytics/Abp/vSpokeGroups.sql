select
rt.SpokeId Spoke_Id,
rv.Level1Group,
rv.Level2Group,
rv.Level3PlusGroups

from {{ source("abp_ref_models", "Rpt_VendorGroup") }} rv
join {{ source("abp_ref_models", "Rpt_TenantVendorGroup") }} rt
on rt.TenantId = rv.TenantId
and rt.VendorGroupId = rv.VendorGroupId and rt.IsDeleted = 0
where rv.IsDeleted = 0