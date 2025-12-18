{{ config(materialized="view") }}

select ra.RiskAssessment_ID, ra.RiskAssessment_TenantId, ra.RiskAssessment_RiskId, tpc.ThirdPartyControl_Label, tpa.ThirdPartyAttributes_Label, tpa.ThirdPartyAttributes_Value
from {{ ref("vwRiskAssessment") }} ra
inner join
    {{ ref("vwRiskAssessmentCustomAttributeData") }} racad
    on racad.RiskAssessmentCustomAttributeData_RiskAssessmentId = ra.RiskAssessment_Id
inner join
    {{ ref("vwThirdPartyAttributes") }} tpa
    on racad.RiskAssessmentCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
inner join
    {{ ref("vwThirdPartyControl") }} tpc on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
where tpc.ThirdPartyControl_EntityType = 4
  union all--updated query to fetch Custom Date Values
select ra.RiskAssessment_ID, ra.RiskAssessment_TenantId, ra.RiskAssessment_RiskId, tpc.ThirdPartyControl_Label, cast(tpa.RiskAssessmentThirdPartyControlFreeText_CustomDateValue as varchar(100)) ThirdPartyAttributes_Label, 0 ThirdPartyAttributes_Value
from {{ ref("vwRiskAssessment") }} ra
inner join
    {{ ref("vwRiskAssessmentThirdPartyControlFreeText") }} tpa
    on ra.RiskAssessment_Id = tpa.RiskAssessmentThirdPartyControlFreeText_RiskAssessmentId
inner join
    {{ ref("vwThirdPartyControl") }} tpc on tpa.RiskAssessmentThirdPartyControlFreeText_ThirdPartyControlId = tpc.ThirdPartyControl_Id
where tpc.ThirdPartyControl_EntityType = 4
and tpa.RiskAssessmentThirdPartyControlFreeText_CustomDateValue is not null