-- with agg as (
select distinct
    cast(VWASSESSMENT.Assessment_ID as varchar)
    + '_'
    + cast(VWASSESSMENT.Assessment_AssessmentVersionName as varchar)
    + '_'
    + VWASSESSMENTDOMAIN.AssessmentDomain_Name as grain_key,
    "VWASSESSMENT"."Assessment_ID",
    'Total' Question_Calculation,
    "VWASSESSMENT"."Assessment_TenantId",
    VWASSESSMENT.Assessment_Name,
    at.Assessment_Name as Template,
    "VWASSESSMENT"."Assessment_AssessmentVersionName",
    "VWASSESSMENTDOMAIN"."AssessmentDomain_Name",
    SUM("VWQUESTIONANSWER"."Answer_Score") over (
        partition by "VWASSESSMENT"."Assessment_ID", "VWASSESSMENTDOMAIN"."AssessmentDomain_ID"
    ) Agg
from {{ ref("vwAssessment") }} at
inner join
    {{ ref("vwAssessment") }} VWASSESSMENT
    on VWASSESSMENT.Assessment_TenantId = at.Assessment_TenantId
    and VWASSESSMENT.Assessment_CreatedFromTemplateId = at.Assessment_ID
    and VWASSESSMENT.Assessment_IsTemplate = 0
    and VWASSESSMENT.Assessment_WorkFlowId = 0
inner join
    {{ ref("vwAssessmentDomain") }} VWASSESSMENTDOMAIN
    on ("VWASSESSMENT"."Assessment_ID" = "VWASSESSMENTDOMAIN"."AssessmentDomain_AssessmentId")
inner join
    {{ ref("vwQuestionAnswer") }} VWQUESTIONANSWER
    on ("VWASSESSMENTDOMAIN"."AssessmentDomain_ID" = "VWQUESTIONANSWER"."Question_AssessmentDomainId")
where
    (
        "VWASSESSMENT"."Assessment_Status" in (4, 6)
        and "VWASSESSMENT"."Assessment_QuestionTypeCode" = 'Weighted Score'
        and "VWASSESSMENT"."Assessment_IsTemplate" = 0
        and "VWASSESSMENT"."Assessment_WorkFlowId" = 0
    )

union all

select distinct
    cast(VWASSESSMENT.Assessment_ID as varchar)
    + '_'
    + cast(VWASSESSMENT.Assessment_AssessmentVersionName as varchar)
    + '_'
    + VWASSESSMENTDOMAIN.AssessmentDomain_Name as grain_key,
    "VWASSESSMENT"."Assessment_ID",
    'Average' Question_Calculation,
    "VWASSESSMENT"."Assessment_TenantId",
    VWASSESSMENT.Assessment_Name,
    at.Assessment_Name as Template,
    "VWASSESSMENT"."Assessment_AssessmentVersionName",
    "VWASSESSMENTDOMAIN"."AssessmentDomain_Name",
    AVG("VWQUESTIONANSWER"."Answer_Score") over (
        partition by "VWASSESSMENT"."Assessment_ID", "VWASSESSMENTDOMAIN"."AssessmentDomain_ID"
    ) Agg
from {{ ref("vwAssessment") }} at
inner join
    {{ ref("vwAssessment") }} VWASSESSMENT
    on VWASSESSMENT.Assessment_TenantId = at.Assessment_TenantId
    and VWASSESSMENT.Assessment_CreatedFromTemplateId = at.Assessment_ID
    and VWASSESSMENT.Assessment_IsTemplate = 0
    and VWASSESSMENT.Assessment_WorkFlowId = 0
inner join
    {{ ref("vwAssessmentDomain") }} VWASSESSMENTDOMAIN
    on ("VWASSESSMENT"."Assessment_ID" = "VWASSESSMENTDOMAIN"."AssessmentDomain_AssessmentId")
inner join
    {{ ref("vwQuestionAnswer") }} VWQUESTIONANSWER
    on ("VWASSESSMENTDOMAIN"."AssessmentDomain_ID" = "VWQUESTIONANSWER"."Question_AssessmentDomainId")
where
    (
        "VWASSESSMENT"."Assessment_Status" in (4, 6)
        and "VWASSESSMENT"."Assessment_QuestionTypeCode" = 'Weighted Score'
        and "VWASSESSMENT"."Assessment_IsTemplate" = 0
        and "VWASSESSMENT"."Assessment_WorkFlowId" = 0
    )
    -- )
    -- select 
    -- grain_key
    -- , Assessment_ID
    -- , Question_Calculation
    -- , Assessment_TenantId
    -- , Assessment_Name
    -- , Template
    -- , Assessment_AssessmentVersionName
    -- , AssessmentDomain_Name
    -- , Agg
    -- , min(Agg) over (partition by "AssessmentDomain_Name" ) Min
    -- , null Max
    -- , null Mean
    -- , 'Min' Roll_Up
    -- from agg
    -- union all
    -- select 
    -- grain_key
    -- , Assessment_ID
    -- , Question_Calculation
    -- , Assessment_TenantId
    -- , Assessment_Name
    -- , Template
    -- , Assessment_AssessmentVersionName
    -- , AssessmentDomain_Name
    -- , Agg
    -- , null Min
    -- , max(Agg) over (partition by "AssessmentDomain_Name" ) Max
    -- , null Mean
    -- , 'Max' Roll_Up
    -- from agg
    -- union all
    -- select 
    -- grain_key
    -- , Assessment_ID
    -- , Question_Calculation
    -- , Assessment_TenantId
    -- , Assessment_Name
    -- , Template
    -- , Assessment_AssessmentVersionName
    -- , AssessmentDomain_Name
    -- , Agg
    -- , null Min
    -- , null Max
    -- , avg(Agg) over (partition by "AssessmentDomain_Name" ) Mean
    -- , 'Mean' Roll_Up
    -- from agg
    
