select
    rcdft.Risk_TenantId,
    rcdft.RiskThirdPartyControlCustomText_RiskId Risk_Id,
    rcdft.CustomLabel,
    cast(rcdft.Value as varchar(2000)) Value
from {{ ref("vwRiskCustomDataFreeText") }} rcdft
