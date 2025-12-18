select distinct
    TOP 10000
    "RISKASSESSMENT"."RiskAssessment_Id" as C2,
    "RISKASSESSMENT"."RiskAssessment_SystemRiskAssessmentCode" as C4,
    "RISK"."Risk_IdRef" as C6,
    "RISK"."Risk_Name" as C8,
    "VWRISKATTRIBUTELIST"."Risk_TagList" as C10,
    "RISKASSESSMENT"."RiskAssessment_Title" as C12,
    "RISKASSESSMENT"."RiskAssessment_Label" as C14,
    "RISKASSESSMENT"."RiskAssessment_SystemRiskAssessmentCode" as C16,
    "RISKASSESSMENTATTRIBUTELIST"."RiskAssessment_AssessmentDate" as C18,
    "RISKASSESSMENTATTRIBUTELIST"."MatrixName" as C20,
    "RISKASSESSMENTATTRIBUTELIST"."Rating" as C22,
    "RISKASSESSMENTATTRIBUTELIST"."Likelihood" as C24,
    "RISKASSESSMENTATTRIBUTELIST"."X_AxisLabel" + ' = ' + "RISKASSESSMENTATTRIBUTELIST"."Likelihood" as C25,
    "RISKASSESSMENTATTRIBUTELIST"."Impact" as C27,
    "RISKASSESSMENTATTRIBUTELIST"."Y_AxisLabel" + ' = ' + "RISKASSESSMENTATTRIBUTELIST"."Impact" as C28
from {{ ref("vwRiskAssessment") }} as "RISKASSESSMENT"
inner join
    {{ ref("vwRiskAssessmentAttributeList") }} as "RISKASSESSMENTATTRIBUTELIST"
    on (
        "RISKASSESSMENT"."RiskAssessment_Id" = "RISKASSESSMENTATTRIBUTELIST"."RiskAssessmentId"
        and "RISKASSESSMENT"."RiskAssessment_TenantId" = "RISKASSESSMENTATTRIBUTELIST"."TenantId"
    )
inner join
    {{ ref("vwRisk") }} as "RISK"
    on (
        "RISK"."Risk_Id" = "RISKASSESSMENT"."RiskAssessment_RiskId"
        and "RISK"."Risk_TenantId" = "RISKASSESSMENT"."RiskAssessment_TenantId"
    )
left outer join
    {{ ref("vwRiskAttributeList") }} as "VWRISKATTRIBUTELIST" on ("RISK"."Risk_Id" = "VWRISKATTRIBUTELIST"."RiskId")
where
    ("RISKASSESSMENT"."RiskAssessment_IsDeleted" = 0)
    and ("RISK"."Risk_TenantId" in (1384))
    and (
        "RISKASSESSMENTATTRIBUTELIST"."MatrixName" in ('Risk Dev')
        and ("RISK"."Risk_Status" = 1)
        and "RISKASSESSMENTATTRIBUTELIST"."Likelihood" in ('3 - Possible')
        and "RISKASSESSMENTATTRIBUTELIST"."Impact" in ('4 - Major')
        and "RISKASSESSMENTATTRIBUTELIST"."Likelihood" is not NULL
        and "RISKASSESSMENTATTRIBUTELIST"."Impact" is not NULL
        and "RISKASSESSMENT"."RiskAssessment_RiskLabelIsCurrent" = 1
    )
