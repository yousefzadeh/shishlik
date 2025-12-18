select
itp.Uuid,
itp.TenantId,
itp.IssueId Issues_Id,
itp.TenantVendorId Issues_LinkedThirdPartyId,
tv.Name Issues_LinkedThirdParty

from {{ source("issue_ref_models", "IssueThirdParty") }} itp
join {{ source("third-party_ref_models", "TenantVendor") }} tv
on tv.Id = itp.TenantVendorId and tv.IsDeleted = 0 and tv.IsArchived = 0
where itp.IsDeleted = 0