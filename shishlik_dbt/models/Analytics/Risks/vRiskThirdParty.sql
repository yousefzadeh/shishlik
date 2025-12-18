select 
rt.Uuid,
rt.TenantId,
rt.RiskId Risk_Id,
rt.TenantVendorId Risk_ThirdPartyId,
tv.Name Risk_LinkedThirdParty

from {{ source("risk_ref_models", "RiskThirdParty") }} rt
join {{ source("third-party_ref_models", "TenantVendor") }} tv
on tv.Id = rt.TenantVendorId and tv.IsDeleted = 0 and tv.IsArchived = 0
where rt.IsDeleted = 0