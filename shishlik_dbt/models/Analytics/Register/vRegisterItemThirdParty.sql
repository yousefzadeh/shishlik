select distinct
itp.TenantId,
itp.IssueId RegisterItem_Id,
itp.TenantVendorId RegisterItem_LinkedThirdPartyId,
tv.Name RegisterItem_LinkedThirdParty

from {{ source("register_ref_models", "IssueThirdParty") }} itp
join {{ source("third-party_ref_models", "TenantVendor") }} tv
on tv.Id = itp.TenantVendorId and tv.IsDeleted = 0 and tv.IsArchived = 0
where itp.IsDeleted = 0