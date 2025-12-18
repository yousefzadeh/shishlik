select
ah.TenantId,
ah.DomainRegisterItemId Asset_Id,
ah.HostIPOrDomain Asset_Domain

from {{ source("asset_ref_models", "AssetHostIPOrDomain") }} ah
where ah.IsDeleted = 0
and ah.DomainRegisterItemId is not null