select
coalesce(t.Id, tpd.TenantId) TenantId,
tpd.ThirdPartyControlId Matrix_Id,
tpc.LabelVarchar X_Axis,
tpa.Id X_Attribute_Id,
tpa.LabelVarchar X_Attribute,
tpa.Value X_Order,
tpd.Id Config_Id--ThirdpartyData

from {{ source("third-party_ref_models", "ThirdPartyDynamicFieldConfiguration") }} tpd
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = tpd.XAxisThirdPartyControlId and tpc.IsDeleted = 0 
join {{ source("third-party_ref_models", "ThirdPartyAttributes") }} tpa on tpc.Id = tpa.ThirdPartyControlId and tpa.IsDeleted = 0
left hash join {{ source("abp_ref_models", "AbpTenants") }} t
on t.ServiceProviderId = tpd.TenantId and t.IsActive = 1 and t.IsDeleted = 0

where tpd.IsDeleted = 0
and tpc.Enabled = 1