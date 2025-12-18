select
coalesce(t.Id, tpd.TenantId) TenantId,
tpd.ThirdPartyControlId Matrix_Id,
tpc.LabelVarchar Matrix_Name,
tpd.Id Config_Id

from {{ source("third-party_ref_models", "ThirdPartyDynamicFieldConfiguration") }} tpd
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = tpd.ThirdPartyControlId and tpc.IsDeleted = 0
left hash join {{ source("abp_ref_models", "AbpTenants") }} t
on t.ServiceProviderId = tpd.TenantId and t.IsActive = 1 and t.IsDeleted = 0

where tpd.IsDeleted = 0