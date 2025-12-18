/**********************

    One row is a question and a choice expanded from the json ComponentStr
    Row count = Question X Choices of each question

    The ComponentStr is expanded based on the Question_Type
    Type 1 (Yes No) - '$.components.input.values'
    Type 2,5,6,10 (Choose One) - '$.components.radiocustom.values'
    Type 3,7,8 (Choose Many) - '$.components.multiselect.values'
    Type 4,9 (Free Text Response) - '$.components'

    Row counts must match with row counts of Question table
    The source tables is taken from source and not from view to be more efficient

 ****************************/
with
    yes_no as (
        select
            q.TenantID Question_TenantId,
            q.ID Question_Id,
            q.AssessmentDomainId Question_AssessmentDomainId,
            cast(q.Type as nvarchar(4000)) Question_Type,
            q.Weighting Question_Weighting,
            js_kv. [key] Question_OptionId,
            json_value(js_kv. [value], '$.label') Question_OptionLabel,
            json_value(js_kv. [value], '$.value') Question_OptionValue,
            coalesce(json_value(js_kv. [value], '$.rank'), 1) Question_OptionRank,
            coalesce(json_value(js_kv. [value], '$.riskStatus'), 0) Question_OptionRiskStatus
        from
            {{ source("assessment_models", "Question") }} q
            cross apply openjson(q.ComponentStr, '$.components.input.values') js_kv
        where q.Type = 1  -- q.Question_TypeCode = 'Yes No' --  
    ),
    ch_one as (
        select
            q.TenantID Question_TenantId,
            q.ID Question_Id,
            q.AssessmentDomainId Question_AssessmentDomainId,
            cast(q.Type as nvarchar(4000)) Question_Type,
            q.Weighting Question_Weighting,
            js_kv. [key] Question_OptionId,
            json_value(js_kv. [value], '$.label') Question_OptionLabel,
            json_value(js_kv. [value], '$.value') Question_OptionValue,
            coalesce(json_value(js_kv. [value], '$.rank'), 1) Question_OptionRank,
            coalesce(json_value(js_kv. [value], '$.riskStatus'), 0) Question_OptionRiskStatus
        from
            {{ source("assessment_models", "Question") }} q
            cross apply openjson(q.ComponentStr, '$.components.radiocustom.values') js_kv
        where q.Type in (2, 5, 6, 10)  -- q.Question_TypeCode = 'Choose One' --  
    ),
    ch_many as (
        select
            q.TenantID Question_TenantId,
            q.ID Question_Id,
            q.AssessmentDomainId Question_AssessmentDomainId,
            cast(q.Type as nvarchar(4000)) Question_Type,
            q.Weighting Question_Weighting,
            js_kv. [key] Question_OptionId,
            json_value(js_kv. [value], '$.label') Question_OptionLabel,
            json_value(js_kv. [value], '$.value') Question_OptionValue,
            coalesce(json_value(js_kv. [value], '$.rank'), 1) Question_OptionRank,
            case
                when coalesce(json_value(js_kv. [value], '$.riskStatus'), 0) = ''
                then 0
                else coalesce(json_value(js_kv. [value], '$.riskStatus'), 0)
            end Question_OptionRiskStatus
        from
            {{ source("assessment_models", "Question") }} q
            cross apply openjson(q.ComponentStr, '$.components.multiselect.values') js_kv
        where q.Type in (3, 7, 8)  -- q.Question_TypeCode = 'Choose Many' --  
    ),
    free_txt as (
        select
            q.TenantID Question_TenantId,
            q.ID Question_ID,
            q.AssessmentDomainId Question_AssessmentDomainId,
            cast(q.Type as nvarchar(4000)) Question_Type,
            q.Weighting Question_Weighting,
            0 Question_OptionId,
            'Free text' Question_OptionLabel,
            'Free text' Question_OptionValue,
            1 Question_OptionRank,
            0 Question_OptionRiskStatus
        from {{ source("assessment_models", "Question") }} q
        where q.Type in (4, 9)
    )

/********* 
Yes No questions are legacy and will not occure going forward
select * from yes_no
UNION ALL 
**********/
select *
from ch_one
union all
select *
from ch_many
union all
select *
from free_txt
