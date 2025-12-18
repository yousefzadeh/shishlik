--check
select
irr.TenantId,
irr.IssueId RegisterItem_Id,
irr.LinkedIssueId RegisterItem_LinkedAssetId,
a.Asset_Name RegisterItem_LinkedAsset

from {{ source("register_ref_models", "IssueRegisterRecord") }} irr
join {{ ref("vAsset") }} a
on a.Asset_Id = irr.LinkedIssueId
where irr.IsDeleted = 0