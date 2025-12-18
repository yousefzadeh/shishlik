{{ config(materialized="view") }}

/**********************************
    For Single Answers
        - TextArea
        - RadioCustom
        - Radio
        - Combined - AnswerText
    For Multiple Answers
        - MultiSelectValues - comma delimited values in one row

**********************************/
with
    base as (
        select
            {{ system_fields_macro() }},
            [AssessmentResponseId],
            [QuestionId],
            [ComponentStr] ComponentStr,
            case when [ComponentStr] = '' then NULL else cast(JSON_VALUE(ComponentStr, '$.RadioCustom') as nvarchar(2000)) end as RadioCustom,
            case when [ComponentStr] = '' then NULL else cast(JSON_VALUE(ComponentStr, '$.Radio') as nvarchar(2000)) end as Radio,
            case when [ComponentStr] = '' then NULL else cast(JSON_VALUE(ComponentStr, '$.TextArea') as nvarchar(4000)) end as TextArea,
            /*
    DEV: Naunghton Williams
    We have noticed that the trim function is not removing the blank spaces.
    We noticed that the TRIM can't compute the spaces but REPLACE can.
    To resolve this, I have converted the all spaces to pipes "|" and then remove all the double spaces
    as these are what are in the extracted json.
    Then we convert back all normal spaces to back to spaces.
    If there are any pipes in the questions then this logic will breack as it will remove the spaces
    */
            case when [ComponentStr] = '' then NULL else 
            cast(
                REPLACE(
                    REPLACE(    
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        REPLACE(JSON_QUERY(ComponentStr, '$.MultiSelectValues'),'[','')  -- remove []
                                    ,']','')
                                ,' ','|'),
                            '||',''),
                        '|',' ')  -- remove double space (replace spaces to | then remove || and replave | with <space>)
                    , '"','') -- remove "
                ,',',', ')as nvarchar(4000) -- replace ',' with ', '
                ) end as MultiSelectValues,
            case when [ComponentStr] = '' then NULL else cast(JSON_VALUE(ComponentStr, '$.Submit') as nvarchar(100)) end as Submit,
            case when [ComponentStr] = '' then NULL else cast(JSON_VALUE(ComponentStr, '$.Id') as nvarchar(200)) end as JsonId,
            [Status],
            case
                [Status] when 1 then 'Published' when 2 then 'In Progress' when 3 then 'Submitted' else 'Others'
            end as [StatusCode],
            [TenantId],
            [MaxPossibleScore],
            [Score],
            [RiskStatus],
            case
                when [RiskStatus] = 0
                then 'No Risk'
                when [RiskStatus] = 6
                then 'Very Low Risk'
                when [RiskStatus] = 1
                then 'Low Risk'
                when [RiskStatus] = 3
                then 'Medium Risk'
                when [RiskStatus] = 4
                then 'High Risk'
                when [RiskStatus] = 5
                then 'Very High Risk'
                else 'Undefined'
            end as [RiskStatusCode],
            case
                when [RiskStatus] = 5
                then 5.0
                when [RiskStatus] = 4
                then 4.0
                when [RiskStatus] = 3
                then 3.0
                when [RiskStatus] = 1
                then 2.0
                when [RiskStatus] = 6
                then 1.0
                when [RiskStatus] = 0
                then 0.0
                else NULL
            end as [RiskStatusCalc],
            [Compliance],
            [ResponderId],
            cast([ReviewerComment] as nvarchar(max)) as [ReviewerComment],
            row_number() over (partition by QuestionId order by Id)[Version],
            case when row_number() over (partition by QuestionId order by Id desc) = 1 then 1 else 0 end as IsCurrent,
            CONCAT(AssessmentResponseId, QuestionId, tenantID) as PK,
            coalesce(LastModificationTime, CreationTime) UpdateTime
        from {{ source("assessment_models", "Answer") }} {{ system_remove_IsDeleted() }}
    ),
    combined as (
        select
            base.*,
            case when [ComponentStr] = '' then NULL 
            else cast(
                COALESCE(
                    RadioCustom, Radio, MultiSelectValues, TextArea  -- must be last
                ) as nvarchar(4000)
            ) end as [Combined],
            -- Final Answer_Explanation
            case when [ComponentStr] = '' then NULL  
                 WHEN RadioCustom  IS NOT NULL OR 
                    Radio IS NOT NULL OR 
                    MultiSelectValues IS NOT NULL
                THEN TextArea
                else NULL
            END AS Explanation
        from base
    )
select
    {{ col_rename("ID", "Answer") }},

    {{ col_rename("CreatorUserId", "Answer") }},
    {{ col_rename("AssessmentResponseId", "Answer") }},
    {{ col_rename("QuestionId", "Answer") }},
    {{ col_rename("ComponentStr", "Answer") }},

    {{ col_rename("RadioCustom", "Answer") }},
    {{ col_rename("Radio", "Answer") }},
    {{ col_rename("MultiSelectValues", "Answer") }},
    {{ col_rename("TextArea", "Answer") }},

    {{ col_rename("Combined", "Answer") }},    
    {{ col_rename("Explanation", "Answer") }},
    {{ col_rename("Submit", "Answer") }},
    {{ col_rename("JsonId", "Answer") }},
    {{ col_rename("Status", "Answer") }},
    {{ col_rename("StatusCode", "Answer") }},

    {{ col_rename("TenantId", "Answer") }},
    {{ col_rename("MaxPossibleScore", "Answer") }},
    {{ col_rename("Score", "Answer") }},
    {{ col_rename("RiskStatus", "Answer") }},
    {{ col_rename("RiskStatusCalc", "Answer") }},
    {{ col_rename("RiskStatusCode", "Answer") }},

    {{ col_rename("Compliance", "Answer") }},
    {{ col_rename("ResponderId", "Answer") }},
    {{ col_rename("ReviewerComment", "Answer") }},
    {{ col_rename("Version", "Answer") }},
    {{ col_rename("IsCurrent", "Answer") }},
    {{ col_rename("PK", "Answer") }},
    {{ col_rename("UpdateTime", "Answer") }}
from combined
