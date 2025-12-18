select
tv.Uuid,
tv.TenantId,
tv.Id ThirdParty_Id,
tv.Name ThirdParty_Name,
tv.CreationTime ThirdParty_CreationTime,
tv.LastModificationTime ThirdParty_LastModificationTime,
tv.VendorId ThirdParty_VendorId,
tv.Criticality ThirdParty_Criticality,
tv.Geography ThirdParty_Geography,
tv.Industry ThirdParty_Industry,
tv.InherentRisk ThirdParty_InherentRisk,
tv.Website ThirdParty_Website,
tv.ContactEmail ThirdParty_ContactEmail

from {{ source("third-party_ref_models", "TenantVendor") }} tv
where tv.IsDeleted = 0
and tv.IsArchived = 0