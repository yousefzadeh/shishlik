-- RiskControl_Controls
-- one row per riskId
select rc.RiskControl_RiskId, c.Controls_Reference, c.Controls_Name
from {{ ref("vwRiskControl") }} rc
inner join {{ ref("vwControls") }} c on rc.RiskControl_ControlId = c.Controls_Id
join {{ ref("vwPolicyDomain") }} pd on pd.PolicyDomain_Id = c.Controls_PolicyDomainId
join {{ ref("vwPolicy") }} p on p.Policy_Id = pd.PolicyDomain_PolicyId and p.Policy_Status != 100
