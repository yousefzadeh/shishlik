with x as (
select distinct
racad.TenantId,
racad.RiskAssessmentId,
tpc.LabelVarchar X_AxisName,
tpa.Id X_AxisValue_Id,
tpa.LabelVarchar X_AxisValue

from {{ source("risk_ref_models", "RiskAssessmentCustomAttributeData") }} racad
join {{ source("third-party_ref_models", "ThirdPartyAttributes") }} tpa
on racad.ThirdPartyAttributesId = tpa.Id and tpa.IsDeleted = 0
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpa.ThirdPartyControlId = tpc.Id and tpc.EntityType = 4 and tpc.Enabled = 1 and tpc.IsDeleted = 0
join {{ source("third-party_ref_models", "ThirdPartyDynamicFieldData") }} tpdfd
on tpdfd.XAxisAttributeId = tpa.Id and tpdfd.IsDeleted = 0
where racad.IsDeleted = 0
)
, y as (
select distinct
racad.TenantId,
racad.RiskAssessmentId,
tpc.LabelVarchar Y_AxisName,
tpa.Id Y_AxisValue_Id,
tpa.LabelVarchar Y_AxisValue

from {{ source("risk_ref_models", "RiskAssessmentCustomAttributeData") }} racad
join {{ source("third-party_ref_models", "ThirdPartyAttributes") }} tpa
on racad.ThirdPartyAttributesId = tpa.Id and tpa.IsDeleted = 0
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpa.ThirdPartyControlId = tpc.Id and tpc.EntityType = 4 and tpc.Enabled = 1 and tpc.IsDeleted = 0
join {{ source("third-party_ref_models", "ThirdPartyDynamicFieldData") }} tpdfd
on tpdfd.YAxisAttributeId = tpa.Id and tpdfd.IsDeleted = 0
where racad.IsDeleted = 0
)
, rating as (
select
racad.TenantId,
racad.RiskAssessmentId,
tpc.Id Matrix_Id,
tpc.LabelVarchar MatrixName,
tpa.Id RatingValue_Id,
tpa.LabelVarchar RatingValue

from {{ source("risk_ref_models", "RiskAssessmentCustomAttributeData") }} racad
join {{ source("third-party_ref_models", "ThirdPartyAttributes") }} tpa
on racad.ThirdPartyAttributesId = tpa.Id
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpa.ThirdPartyControlId = tpc.Id and tpc.EntityType = 4 and tpc.Enabled = 1 and tpc.Type = 2
where racad.IsDeleted = 0
)

select
ra.TenantId,
ra.RiskAssessment_RiskId,
ra.RiskAssessment_Id,
ra.RiskAssessment_Name,
x.X_AxisName RiskAssessment_X_AxisName,
x.X_AxisValue_Id,
x.X_AxisValue RiskAssessment_X_AxisValue,
y.Y_AxisName RiskAssessment_Y_AxisName,
y.Y_AxisValue_Id,
y.Y_AxisValue RiskAssessment_Y_AxisValue,
r.Matrix_Id,
case when r.MatrixName is NULL then 'Risk Rating' else r.MatrixName end RiskAssessment_MatrixName,
r.RatingValue_Id,
case when r.RatingValue is NULL then 'No Risk' else r.RatingValue end RiskAssessment_RatingValue
from {{ ref("vRiskAssessment") }} ra
left join x on x.RiskAssessmentId = ra.RiskAssessment_Id
left join y on y.RiskAssessmentId = ra.RiskAssessment_Id
left join rating r on r.RiskAssessmentId = ra.RiskAssessment_Id