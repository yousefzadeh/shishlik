with ris_trp as (
select
rtp.TenantId,
rtp.Risk_Id,
rtp.RiskTreatmentPlan_Id,
rtp.RiskTreatmentPlan_Name
from {{ ref("vRiskTreatmentPlan") }} rtp
)
, assignee as (
select
rtpa.TenantId, rtpa.RiskTreatmentPlan_Id, string_agg(rtpa.RiskTreatmentPlan_AssigneeName, '; ') RiskTreatmentPlan_AssigneeList
from {{ ref("vRiskTreatmentPlanAssignee") }} rtpa
group by
rtpa.TenantId, rtpa.RiskTreatmentPlan_Id
)

select
rtp.TenantId,
rtp.Risk_Id,
rtp.RiskTreatmentPlan_Id,
rtp.RiskTreatmentPlan_Name,
a.RiskTreatmentPlan_AssigneeList

from ris_trp rtp
left join assignee a on a.RiskTreatmentPlan_Id = rtp.RiskTreatmentPlan_Id