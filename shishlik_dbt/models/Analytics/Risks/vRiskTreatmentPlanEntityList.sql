with ris_trp as (
select
rtp.TenantId,
rtp.Risk_Id,
rtp.RiskTreatmentPlan_Id,
rtp.RiskTreatmentPlan_Name
from {{ ref("vRiskTreatmentPlan") }} rtp
)
, ctrl_set as (
select
rtpp.TenantId, rtpp.RiskTreatmentPlan_Id, string_agg(rtpp.RiskTreatmentPlan_PolicyName, '; ') RiskTreatmentPlan_ControlSetList
from {{ ref("vRiskTreatmentPlanPolicy") }} rtpp
group by
rtpp.TenantId, rtpp.RiskTreatmentPlan_Id
)
, ctrl as (
select
rtpc.TenantId, rtpc.RiskTreatmentPlan_Id, string_agg(rtpc.RiskTreatmentPlan_ControlName, '; ') RiskTreatmentPlan_ControlList
from {{ ref("vRiskTreatmentPlanControl") }} rtpc
group by
rtpc.TenantId, rtpc.RiskTreatmentPlan_Id
)
, auth_prov as (
select
rtap.TenantId, rtap.RiskTreatmentPlan_Id,
string_agg(rtap.RiskTreatmentPlan_AuthorityName, '; ') RiskTreatmentPlan_AuthorityList,
string_agg(rtap.RiskTreatmentPlan_AuthorityProvisionName, '; ') RiskTreatmentPlan_AuthorityProvisionList
from {{ ref("vRiskTreatmentPlanAuthorityProvision") }} rtap
group by
rtap.TenantId, rtap.RiskTreatmentPlan_Id
)

select
rt.TenantId,
rt.Risk_Id,
rt.RiskTreatmentPlan_Id,
rt.RiskTreatmentPlan_Name,
cs.RiskTreatmentPlan_ControlSetList,
c.RiskTreatmentPlan_ControlList,
ap.RiskTreatmentPlan_AuthorityList,
ap.RiskTreatmentPlan_AuthorityProvisionList

from ris_trp rt
left join ctrl_set cs on cs.RiskTreatmentPlan_Id = rt.RiskTreatmentPlan_Id
left join ctrl c on c.RiskTreatmentPlan_Id = rt.RiskTreatmentPlan_Id
left join auth_prov ap on ap.RiskTreatmentPlan_Id = rt.RiskTreatmentPlan_Id