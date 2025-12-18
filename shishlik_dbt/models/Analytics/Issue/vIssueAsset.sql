select
irr.TenantId,
irr.IssueId Issues_Id,
irr.LinkedIssueId Issues_LinkedAssetId,
a.Asset_Name Issues_LinkedAsset

from {{ source("issue_ref_models", "IssueRegisterRecord") }} irr
join {{ ref("vAsset") }} a
on a.Asset_Id = irr.LinkedIssueId
where irr.IsDeleted = 0