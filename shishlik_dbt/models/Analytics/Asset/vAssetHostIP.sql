select distinct
ah.TenantId,
ah.HostIPRegisterItemId Asset_Id,
ah.HostIPOrDomain Asset_HostIP

from {{ source("asset_ref_models", "AssetHostIPOrDomain") }} ah
where ah.IsDeleted = 0
and ah.HostIPRegisterItemId is not null