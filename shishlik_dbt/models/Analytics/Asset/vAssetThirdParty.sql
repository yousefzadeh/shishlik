select distinct
itp.TenantId,
itp.IssueId Asset_Id,
itp.TenantVendorId Asset_LinkedThirdPartyId,
tv.Name Asset_LinkedThirdParty

from {{ source("asset_ref_models", "IssueThirdParty") }} itp
join {{ source("third-party_ref_models", "TenantVendor") }} tv
on tv.Id = itp.TenantVendorId and tv.IsDeleted = 0 and tv.IsArchived = 0
where itp.IsDeleted = 0