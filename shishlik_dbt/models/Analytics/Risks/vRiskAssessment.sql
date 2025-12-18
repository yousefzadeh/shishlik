with ris_asses as (
select
ra.Uuid,
ra.TenantId,
ra.RiskId,
ra.Id RiskAssessment_Id,
ra.Title,
ra.CreationTime,
ra.AssessmentDate
from {{ source("risk_ref_models", "RiskAssessment") }} ra
where ra.IsDeleted = 0
)
, ris_assess_lbl as (
select
rac.TenantId,
rac.RiskAssessmentId,
tpa.Label RiskAssessment_Label
from {{ source("risk_ref_models", "RiskAssessmentCustomAttributeData") }} rac
join {{ source("third-party_ref_models", "ThirdPartyAttributes") }} tpa
on tpa.Id = rac.ThirdPartyAttributesId and tpa.IsDeleted = 0
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = tpa.ThirdPartyControlId and tpc.IsDeleted = 0 and tpc.Name = 'Risk Assessment Labels'
where rac.IsDeleted = 0
)
, final as (
select
ra.TenantId,
ra.RiskId RiskAssessment_RiskId,
ra.RiskAssessment_Id,
case
when ral.RiskAssessment_Label is null then 'No label'
else ral.RiskAssessment_Label
end RiskAssessment_Label,
case when ROW_NUMBER() over (partition by ra.RiskId,
case
when ral.RiskAssessment_Label is null then 'No label' else ral.RiskAssessment_Label end 
order by ra.CreationTime desc) = 1 then 1 else 0 end RiskAssessment_LabelLatestFlag,
ra.Title RiskAssessment_Name,
ra.CreationTime RiskAssessment_CreationTime,
ra.AssessmentDate RiskAssessment_Date
from ris_asses ra
left join ris_assess_lbl ral
on ral.RiskAssessmentId = ra.RiskAssessment_Id
)

select
TenantId,
RiskAssessment_RiskId,
RiskAssessment_Id,
RiskAssessment_Label,
RiskAssessment_LabelLatestFlag,
RiskAssessment_Name,
RiskAssessment_CreationTime,
RiskAssessment_Date
from final