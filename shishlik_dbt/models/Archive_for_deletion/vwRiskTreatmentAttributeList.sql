{{ config(materialized="view") }}

{# /*
 * Risk Title
 * Risk Description
 * Risk Tags
 * Treatment Decision
 * Treatment Status
 * Treatment Plan Status
 * Treatment Plan Name
 * Treatment Plan Description
 * Planned Controls
 * Planned Provision
 * Treatment plan owner
 * Treatment plan due date
 */ #}
with
    risk as (
        select Risk_Id, Risk_TenantId, Risk_Name Risk_Title, Risk_Description
        {# Risk_Status,
	Risk_StatusCode,
	Risk_IdRef,
	Risk_AbstractRiskId,
	Risk_RiskReviewId,
	Risk_RiskCategoryId,
	Risk_CommonCause,
	Risk_LikelyImpact,
	Risk_IdentifiedBy,
	Risk_FavouriteRiskAssessmentId,
	Risk_IsCurrent #}
        from {{ ref("vwRisk") }}
    ),
    rtag as (
        {#     
	-- RiskTag_Tags
    -- one row for each RiskId 
    #}
        select rt.RiskTag_RiskId, STRING_AGG(t.Tags_Name, ',') as RiskTagList
        from {{ ref("vwRiskTag") }} rt
        inner join {{ ref("vwTags") }} t on rt.RiskTag_TagId = t.Tags_ID
        group by rt.RiskTag_RiskId
    ),
    trt as (
        {# one row for each Risk treatment #}
        select
            rt.RiskTreatment_Id,
            rt.RiskTreatment_RiskId,
            rt.RiskTreatment_TenantId,
            rt.RiskTreatment_StatusCode,
            rt.RiskTreatment_DecisionId,
            sl.StatusLists_Name RiskTreatment_DecisionCode,
            sl.StatusLists_Reference,
            sl.StatusLists_IsClosedActionStatus,
            sl.StatusLists_StatusOrder
        from {{ ref("vwRiskTreatment") }} rt
        join {{ ref("vwStatusLists") }} sl on rt.RiskTreatment_DecisionId = sl.StatusLists_Id
    )

select
    risk.Risk_Id,
    risk.Risk_TenantId,
    risk.Risk_Title,
    risk.Risk_Description,
    rtag.RiskTagList,
    trt.RiskTreatment_StatusCode,
    trt.RiskTreatment_DecisionCode
from risk
left join rtag on risk.Risk_Id = rtag.RiskTag_RiskId
left join trt on risk.Risk_Id = trt.RiskTreatment_RiskId
