with custdd as (
select
racad.TenantId,
racad.RiskAssessmentId RiskAssessment_Id,
tpc.LabelVarchar CustomField,
tpa.LabelVarchar CustomFieldvalue,
NULL CustomFieldDateValue

from {{ source("risk_ref_models", "RiskAssessmentCustomAttributeData") }} racad
join {{ source("third-party_ref_models", "ThirdPartyAttributes") }} tpa
on racad.ThirdPartyAttributesId = tpa.Id and tpa.IsDeleted = 0
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpa.ThirdPartyControlId = tpc.Id and tpc.EntityType = 4 and tpc.Enabled = 1 and tpc.IsDeleted = 0

where racad.IsDeleted = 0
)
, custxt as (
select
rt.TenantId,
rt.RiskAssessmentId RiskAssessment_Id,
tpc.LabelVarchar CustomField,
rt.TextData CustomFieldvalue,
NULL CustomFieldDateValue

from {{ source("risk_ref_models", "RiskAssessmentThirdPartyControlFreeText") }} rt
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = rt.ThirdPartyControlId and tpc.IsDeleted = 0 and tpc.Enabled = 1
where rt.TextData is not null

)
, cusdate as (
select
rt.TenantId,
rt.RiskAssessmentId RiskAssessment_Id,
tpc.LabelVarchar CustomField,
NULL CustomFieldvalue,
rt.CustomDateValue CustomFieldDateValue

from {{ source("risk_ref_models", "RiskAssessmentThirdPartyControlFreeText") }} rt
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = rt.ThirdPartyControlId and tpc.IsDeleted = 0 and tpc.Enabled = 1
where rt.CustomDateValue is not null

)

, final as(
select
TenantId,
RiskAssessment_Id,
CustomField,
CustomFieldvalue,
CustomFieldDateValue
from custdd

union

select
TenantId,
RiskAssessment_Id,
CustomField,
CustomFieldvalue,
CustomFieldDateValue
from custxt

union

select
TenantId,
RiskAssessment_Id,
CustomField,
CustomFieldvalue,
CustomFieldDateValue
from cusdate
)

select
TenantId,
RiskAssessment_Id,
CustomField,
CustomFieldvalue,
CustomFieldDateValue

from final