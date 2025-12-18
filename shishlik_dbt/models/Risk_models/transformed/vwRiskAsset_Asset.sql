-- RiskAsset_Asset
-- One row per riskId
select IssueRisk_RiskId RiskAsset_RiskId, Asset_Id, Asset_Title
from
    (
        select distinct ra.IssueRisk_RiskId, a.Asset_Id, a.Asset_Title
        from {{ ref("vwIssueRisk") }} ra
        inner join {{ ref("vwAsset") }} a on ra.IssueRisk_IssueId = a.Asset_Id
    ) as T
