select
coalesce(t.Id, tpd.TenantId) TenantId,
tpd.ThirdPartyControlId Matrix_Id,
tpc.LabelVarchar Y_Axis,
tpa.Id Y_Attribute_Id,
tpa.LabelVarchar Y_Attribute,
tpa.Value Y_Order,
tpd.Id Config_Id--ThirdpartyData

from {{ source("third-party_ref_models", "ThirdPartyDynamicFieldConfiguration") }} tpd
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = tpd.YAxisThirdPartyControlId and tpc.IsDeleted = 0 
join {{ source("third-party_ref_models", "ThirdPartyAttributes") }} tpa on tpc.Id = tpa.ThirdPartyControlId and tpa.IsDeleted = 0
left hash join {{ source("abp_ref_models", "AbpTenants") }} t
on t.ServiceProviderId = tpd.TenantId and t.IsActive = 1 and t.IsDeleted = 0

where tpd.IsDeleted = 0
and tpc.Enabled = 1