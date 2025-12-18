{{ config(materialized="view") }}

/*
 * Multi valued attribute list of multi grained Risk and RiskAssessment
 * Risks +
 * Tags +
 * Assessments +
 * Custom Attributes - Likelihood, Impact, Rating
 * Risk Assessment Tags 
 * Assoc Controls +
 * Associated Provisions +
 * Associated Assets +
 * Associated Assessments name, description +
 * Associated questions
 * Associated responses
 * Associated vendors (Thirdparty) +
 */
with
    grain_risk as (
        -- -  Risk 
        select * from {{ ref("vwRiskAttributeList") }}
    ),
    grain_riskassessment as (
        -- RiskAssessment
        select * from {{ ref("vwRiskAssessmentAttributeList") }}
    )

select
    r.RiskId,
    ra.RiskAssessmentId,
    r.Risk_Name,
    r.Risk_Description,
    r.Risk_TagList,
    ra.RiskAssessment_Title,
    ra.Likelihood,
    ra.Impact,
    ra.Rating,
    ra.RiskAssessment_AssessmentDate,
    ra.RatingDescription,
    ra.RiskAssessment_TagList,
    r.Risk_ControlList,
    r.Risk_AuthorityList,
    r.Risk_AssetList,
    r.Risk_AssessmentList,
    r.Risk_DomainList,
    r.Risk_QuestionList,
    r.Risk_ResponseList,
    r.Risk_VendorList
from grain_risk r
left join grain_riskassessment ra on r.RiskId = ra.RiskId
