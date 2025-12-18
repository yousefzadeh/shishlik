select
rtpa.Uuid,
rtpa.TenantId,
rtpa.RiskId Risk_Id,
rtp.Id RiskTreatmentPlan_Id,
rtp.CreationTime RiskTreatmentPlan_CreationTime,
rtp.TreatmentName RiskTreatmentPlan_Name,
rtp.TreatmentDescription RiskTreatmentPlan_Description,
rtp.Status RiskTreatmentPlan_StatusId,
case
when rtp.Status = 0 then 'New'
when rtp.Status = 1 then 'Completed'
when rtp.Status = 3 then 'In-Progress'
end as RiskTreatmentPlan_Status,
rtp.TreatmentDate RiskTreatmentPlan_DueDate,
rtp.TreatmentCompletedDate RiskTreatmentPlan_CompletedDate

from {{ source("risk_ref_models", "RiskTreatmentPlanAssociation") }} rtpa
join {{ source("risk_ref_models", "RiskTreatmentPlan") }} rtp
on rtp.Id = rtpa.RiskTreatmentPlanId and rtp.IsDeleted = 0 and rtp.IsDeprecated = 0
where rtpa.IsDeleted = 0