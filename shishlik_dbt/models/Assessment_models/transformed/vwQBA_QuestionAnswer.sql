    {{ config(materialized="view") -}}
with
    a_raw as (
        select
            Id,
            cast(coalesce(a.LastModificationTime, a.CreationTime) as smalldatetime) Answer_LastModificationTime,
            QuestionId,
            case when ComponentStr = '' then NULL else  ComponentStr 
                    end as ComponentStr,
            TenantId,
            -- Score, RiskStatus, MaxPossibleScore is correct for single answer questions
            -- Need to calculate from unnested json for multi-answer questions 
            MaxPossibleScore,
            Score,
            RiskStatus,
            case
                RiskStatus
                when 0
                then 'No Risk'
                when 6
                then 'Very Low Risk'
                when 1
                then 'Low Risk'
                when 3
                then 'Medium Risk'
                when 4
                then 'High Risk'
                when 5
                then 'Very High Risk'
                else 'Undefined'
            end as RiskStatusCode,
            case
                RiskStatus
                when 0
                then 0.0
                when 6
                then 1.0
                when 1
                then 2.0
                when 3
                then 3.0
                when 4
                then 4.0
                when 5
                then 5.0
                else cast(NULL as decimal)
            end as RiskStatusCalc,
            --
            Compliance,
            case Compliance when 0 then 'None' when 1 then 'Compliant' when 2 then 'Not compliant' when 3 then 'Partially compliant' else 'Undefined' end as ComplianceCode,
            ResponderId,
            AssessmentResponseId,
            ReviewerComment
        from {{ source("assessment_models", "Answer") }} a
        where IsDeleted = 0 and [Status] = 3
    ),
    a as (
        select
            ID as Answer_ID,
            Answer_LastModificationTime,
            QuestionId as Answer_QuestionId,
            COALESCE(
                ComponentStr,
                '{"RadioCustom":null,"Radio":null,"TextArea":null,"Submit":false,"MultiSelectValues":null,"Id":0}'
            ) as Answer_ComponentStr,
            TenantId as Answer_TenantId,
            MaxPossibleScore as Answer_MaxPossibleScore,
            Score as Answer_Score,
            RiskStatus as Answer_RiskStatus,
            RiskStatusCalc as Answer_RiskStatusCalc,
            RiskStatusCode as Answer_RiskStatusCode,
            Compliance as Answer_Compliance,
            ComplianceCode as Answer_ComplianceCode,
            JSON_VALUE(ComponentStr, '$.Submit') as Answer_Submit,
            ResponderId as Answer_ResponderId,
            AssessmentResponseId as Answer_AssessmentResponseId,
            ReviewerComment as Answer_ReviewerComment
        from a_raw
    ),
    q_raw as (  -- dbo.Question with casting

        {# 
    Question.Type in (3,7,8) for "Choose Many" type of questions
    Question.Type not in (3,7,8) for "Choose Many" type of questions

    Performance tuning:
    * WHERE Type in (3,7,8) will cause inconsistent plans because of of EQUALITY ARRAY operator
    * WHERE Type = 3 UNION ALL WHERE Type = 7 UNION ALL WHERE Type = 8
        - Consistent query plans because we changed the EQUALITY ARRAY into EQUALITY SCALAR operator
        - For choose many loop 3 times, except choose many will loop 7 times, total of always taking 10 loops
        - Loop 10 times in all cases
    * Create calculated column IsMultiSelectType and index it
        - WHERE IsMultiSelectType = 0 and WHERE IsMultiSelectType = 1 are both EQUALITY SCALAR
        - Consistent plans, single loop
        - Most efficient
#}
        select
            q.TenantId Question_TenantId,
            q.id Question_Id,
            cast(coalesce(q.LastModificationTime, q.CreationTime) as smalldatetime) as Question_LastModificationTime,
            q.[Order] Question_Order,
            q.IdRef Question_IdRef,
            cast(q.Name as nvarchar(4000)) Question_Name,
            q.Description Question_Description,
            q.AssessmentDomainId Question_AssessmentDomainId,
            [Type] Question_Type,
            case [Type] -- 1,2,3,4,5,6,7,8,9,10
            when 1 then 'Yes No'
            when 2 then 'Custom'
            when 3 then 'Multiple Choice'
            when 4 then 'Text Response'
            when 5 then 'Geography'
            when 6 then 'Industry'
            when 7 then 'Multiple Choice Geography'
            when 8 then 'Multiple Choice Industry'
            when 9 then 'Short Text Response'
            when 10 then 'Custom Radio'
            else 'Undefined'
            end as Question_TypeCode,
            {# 
                If IsMultiSelect column is created then use IsMultiSelectType column in this expression 
                IsMultiSlecetType as Question_IsMultiSelectType, 
            #}
            case when Type in (3, 7, 8) then 1 else 0 end Question_IsMultiSelectType,
            q.Weighting Question_Weighting,
            json_modify(
                case
                    when Type in (2, 5, 6, 10)
                    then json_query(q.ComponentStr, '$.components.radiocustom.values')
                    when Type in (3, 7, 8)
                    then json_query(q.ComponentStr, '$.components.multiselect.values')
                end,
                'append $',
                json_query('{ "value": "NULL" }')
            ) Question_OptionJson,
            HasConditionalLogic Question_HasConditionalLogic,
            HiddenInSurveyForConditional Question_HiddenInSurveyForConditional,
            QuestionGroupId Question_QuestionGroupId,
            QuestionGroupResponseId Question_QuestionGroupResponseId,
            IsMandatory Question_IsMandatory,
            DisplayDocumentUpload Question_DisplayDocumentUpload
        from {{ source("assessment_models", "Question") }} q
        where IsDeleted = 0
    ),
    q as (  -- Tidy question option json based on question type
        select
            q.Question_TenantId,
            q.Question_Id,
            q.Question_LastModificationTime,
            q.Question_Order,
            q.Question_IdRef,
            q.Question_Name, 
            q.Question_Description,
            q.Question_AssessmentDomainId,
            q.Question_Type,
            q.Question_IsMultiSelectType,
            q.Question_TypeCode,
            Question_Weighting,
            Question_OptionJson,
            Question_HasConditionalLogic,
            Question_HiddenInSurveyForConditional,
            Question_QuestionGroupId,
            Question_QuestionGroupResponseId,
            Question_IsMandatory,
            Question_DisplayDocumentUpload
        from q_raw q
    ),
    qq as (  -- unnest question to get Rank and weighting to calculate score and max possible scoree
        select q.Question_Id, q.Question_Weighting, QuestionOption_Value, QuestionOption_Rank, QuestionOption_RiskStatus
        from q outer apply openjson(Question_OptionJson, '$')
        with
            (
                QuestionOption_Value nvarchar(4000) '$.value',
                QuestionOption_Rank INT '$.rank',
                QuestionOption_RiskStatus INT '$.riskStatus'
            ) question_kv
        where q.Question_Type in (3, 7, 8)
    )
   ,qa_multi_json as (
        -- join question to answer for multi response type
        -- tidy answer json based on question type.
        -- prepare answer json as an array for cross apply openjson()
        select
            Question_TenantId,
            Question_Id,
            Question_LastModificationTime,
            Question_Order,
            Question_IdRef,
            Question_Name, 
            Question_Description,
            Question_AssessmentDomainId,
            Question_Type,
            Question_TypeCode,
            Question_Weighting,
            Question_HasConditionalLogic,
            Question_HiddenInSurveyForConditional,
            Question_QuestionGroupId,
            Question_QuestionGroupResponseId,
            Question_IsMandatory,
            Question_DisplayDocumentUpload,
            Answer_QuestionId,
            Answer_Id,
            Answer_LastModificationTime,
            Answer_Compliance,
            Answer_ComplianceCode,
            1 Answer_ResponseCount,
            -- -
            Answer_Score,
            Answer_RiskStatus,
            Answer_RiskStatusCode,
            Answer_RiskStatusCalc,
            Answer_MaxPossibleScore,
            '{ "answer" : ' + json_query(a.Answer_ComponentStr, '$.MultiSelectValues') + ' }' Answer_ResponseJson,
            -- json array for cross apply openjson()
            cast(json_value(a.Answer_ComponentStr, '$.TextArea') as nvarchar(4000)) Answer_TextArea,
            Answer_Submit,
            Answer_ResponderId,
            Answer_AssessmentResponseId,
            Answer_ReviewerComment
        from q
        join a on q.Question_Id = a.Answer_QuestionId
        where Question_IsMultiSelectType = 1 and a.Answer_ComponentStr is not NULL
    ),
    -- - Start of union tables
    qa_no_answer as (
        -- Question with no answer - score is zero
        select
            Question_TenantId,
            Question_Id,
            Question_LastModificationTime,
            Question_Order,
            Question_IdRef,
            Question_Name, 
            Question_Description,
            Question_AssessmentDomainId,
            Question_Type,
            Question_TypeCode,
            Question_Weighting,
            Question_HasConditionalLogic,
            Question_HiddenInSurveyForConditional,
            Question_QuestionGroupId,
            Question_QuestionGroupResponseId,
            Question_IsMandatory,
            Question_DisplayDocumentUpload,
            Question_Id Answer_QuestionId,
            0 Answer_Id,
            cast(NULL as datetime) Answer_LastModificationTime,
            0 Answer_Compliance,
            'None' Answer_ComplianceCode,
            0 Answer_ResponseCount,
            0 Answer_Score,
            cast(NULL as INT) Answer_RiskStatus,
            'Undefined' Answer_RiskStatusCode,
            cast(NULL as decimal(7,2)) Answer_RiskStatusCalc,
            cast(NULL as INT) Answer_MaxPossibleScore,
            cast(NULL as INT) AnswerResponse_key,
            'Not Responded' as AnswerResponse_Value,
            'Not Responded' as Answer_TextArea,
            '' Answer_Submit,
            cast(NULL as INT) Answer_ResponderId,
            cast(NULL as INT) Answer_AssessmentResponseId,
            '' Answer_ReviewerComment,
            '0_0' AnswerResponse_PK
        from q
        left join a on q.Question_Id = a.Answer_QuestionId
        where Answer_Id is NULL
    ),
    qa_freetext as (
        -- Question with Answer Type is Free text 4,9, score = NULL
        select
            Question_TenantId,
            Question_Id,
            Question_LastModificationTime,
            Question_Order,
            Question_IdRef,
            Question_Name, 
            Question_Description,
            Question_AssessmentDomainId,
            Question_Type,
            Question_TypeCode,
            Question_Weighting,
            Question_HasConditionalLogic,
            Question_HiddenInSurveyForConditional,
            Question_QuestionGroupId,
            Question_QuestionGroupResponseId,
            Question_IsMandatory,
            Question_DisplayDocumentUpload,
            Answer_QuestionId,
            Answer_Id,
            Answer_LastModificationTime,
            Answer_Compliance,
            Answer_ComplianceCode,
            1 Answer_ResponseCount,
            cast(NULL as decimal(15,2)) Answer_Score,
            Answer_RiskStatus,
            Answer_RiskStatusCode,
            Answer_RiskStatusCalc,
            Answer_MaxPossibleScore,
            0 AnswerResponse_key,
            cast(
                case
                    when json_value(a.Answer_ComponentStr, '$.TextArea') = ''
                    then 'Blank'
                    else json_value(a.Answer_ComponentStr, '$.TextArea')
                end as nvarchar(4000)
            ) as AnswerResponse_Value,
            NULL as Answer_TextArea,
            Answer_Submit,
            Answer_ResponderId,
            Answer_AssessmentResponseId,
            Answer_ReviewerComment,
            cast(a.Answer_Id as varchar(10)) + '_0' AnswerResponse_PK
		from q
		join a on q.Question_Id = a.Answer_QuestionId
        where Question_Type in (4, 9)
    ),
    qa_single as (
        -- Question with Answer of Single Answer type
        select
            Question_TenantId,
            Question_Id,
            Question_LastModificationTime,
            Question_Order,
            Question_IdRef,
            Question_Name, 
            Question_Description,
            Question_AssessmentDomainId,
            Question_Type,
            Question_TypeCode,
            Question_Weighting,
            Question_HasConditionalLogic,
            Question_HiddenInSurveyForConditional,
            Question_QuestionGroupId,
            Question_QuestionGroupResponseId,
            Question_IsMandatory,
            Question_DisplayDocumentUpload,
            Answer_QuestionId,
            Answer_Id,
            Answer_LastModificationTime,
            Answer_Compliance,
            Answer_ComplianceCode,
            1 Answer_ResponseCount,
            Answer_Score,
            Answer_RiskStatus,
            Answer_RiskStatusCode,
            Answer_RiskStatusCalc,
            Answer_MaxPossibleScore,
            0 AnswerResponse_key,
            cast(
                case
                    when Question_Type in (1)
                    then json_value(a.Answer_ComponentStr, '$.Radio')
                    when Question_Type in (2, 5, 6, 10)
                    then json_value(a.Answer_ComponentStr, '$.RadioCustom')
                end as nvarchar(4000)
            ) as AnswerResponse_Value,
            cast(json_value(a.Answer_ComponentStr, '$.TextArea') as nvarchar(4000)) Answer_TextArea,
            Answer_Submit,
            Answer_ResponderId,
            Answer_AssessmentResponseId,
            Answer_ReviewerComment,
            cast(a.Answer_Id as varchar(10)) + '_0' AnswerResponse_PK
        from q
        join a on q.Question_Id = a.Answer_QuestionId
        where Question_IsMultiSelectType = 0 and a.Answer_ComponentStr is not NULL and Question_Type in (1, 2, 5, 6, 10)
    ),
    qa_multi as (
        select
            qa.Question_TenantId,
            qa.Question_Id,
            qa.Question_LastModificationTime,
            qa.Question_Order,
            qa.Question_IdRef,
            qa.Question_Name, 
            qa.Question_Description,
            qa.Question_AssessmentDomainId,
            qa.Question_Type,
            qa.Question_TypeCode,
            qa.Question_Weighting,
            qa.Question_HasConditionalLogic,
            qa.Question_HiddenInSurveyForConditional,
            qa.Question_QuestionGroupId,
            qa.Question_QuestionGroupResponseId,
            qa.Question_IsMandatory,
            qa.Question_DisplayDocumentUpload,
            qa.Answer_QuestionId,
            qa.Answer_Id,
            qa.Answer_LastModificationTime,
            qa.Answer_Compliance,
            qa.Answer_ComplianceCode,
            qa.Answer_ResponseCount,
            cast(qq.Question_Weighting * qq.QuestionOption_Rank as decimal(15, 2)) Answer_Score,
            qq.QuestionOption_RiskStatus Answer_RiskStatus,
            case
                qq.QuestionOption_RiskStatus
                when 0
                then 'No Risk'
                when 6
                then 'Very Low Risk'
                when 1
                then 'Low Risk'
                when 3
                then 'Medium Risk'
                when 4
                then 'High Risk'
                when 5
                then 'Very High Risk'
                when NULL 
                then 'Undefined'
                else 'Undefined'
            end as RiskStatusCode,
            case
                qq.QuestionOption_RiskStatus
                when 0
                then 0.0
                when 6
                then 1.0
                when 1
                then 2.0
                when 3
                then 3.0
                when 4
                then 4.0
                when 5
                then 5.0
                else cast(NULL as decimal(7, 2))
            end as RiskStatusCalc,
            sum(CAST(qq.Question_Weighting as bigint) * CAST(qq.QuestionOption_Rank as bigint)) over (
                partition by qq.Question_Id
            ) Question_MaxPossibleScore,
            cast(answer_kv. [key] + 1 as int) AnswerResponse_key,
            cast(answer_kv.value as nvarchar(4000)) as AnswerResponse_Value,
            qa.Answer_TextArea,
            Answer_Submit,
            Answer_ResponderId,
            Answer_AssessmentResponseId,
            Answer_ReviewerComment,
            concat(qa.Answer_Id, '_', answer_kv. [key]) AnswerResponse_PK
        from qa_multi_json qa outer apply openjson(qa.Answer_ResponseJson, '$.answer') as answer_kv
        left join qq on qa.Question_Id = qq.Question_Id and answer_kv.value = qq.QuestionOption_Value
    ),
    QuestionGroup as   (
        select
            Id as QuestionGroup_Id
        from {{ source("assessment_models", "QuestionGroup") }} {{ system_remove_IsDeleted() }}
    ),
    QuestionGroupResponse as   (
        select
            Id as QuestionGroupResponse_ID,
			IdRef as QuestionGroupResponse_IdRef,
			Response as QuestionGroupResponse_Response,
			Compliance as QuestionGroupResponse_Compliance,
			AssessmentResponseId as QuestionGroupResponse_AssessmentResponseId
        from {{ source("assessment_models", "QuestionGroupResponse") }} {{ system_remove_IsDeleted() }}
    ),
    qa_group_single as (
        select 
            Answer_TenantId Question_TenantId,
            Question_ID,
            Question_LastModificationTime,
            Question_Order,
            qgr.QuestionGroupResponse_IdRef as Question_IdRef,
            Question_Name,
            Question_Description,
            Question_AssessmentDomainId,
            Question_Type,
            Question_TypeCode,
            Question_Weighting,
            Question_HasConditionalLogic,
            Question_HiddenInSurveyForConditional,
            Question_QuestionGroupId,
            Question_QuestionGroupResponseId,
            Question_IsMandatory,
            coalesce(Question_DisplayDocumentUpload,0) as Question_DisplayDocumentUpload,
            Answer_QuestionId,
            Answer_ID,
            Answer_LastModificationTime,
            qgr.QuestionGroupResponse_Compliance as Answer_Compliance,
            case qgr.QuestionGroupResponse_Compliance when 0 then 'None' when 1 then 'Compliant' when 2 then 'Not compliant' when 3 then 'Partially compliant' else 'Undefined' end as Answer_ComplianceCode,
            1 as Answer_ResponseCount,
            Answer_Score,
            Answer_RiskStatus,
            Answer_RiskStatusCode,
            Answer_RiskStatusCalc,
            Answer_MaxPossibleScore,
            1 as AnswerResponse_key,
            cast(qgr.QuestionGroupResponse_Response as nvarchar(4000)) as AnswerResponse_Value,
            cast(json_value(a.Answer_ComponentStr, '$.TextArea') as nvarchar(4000)) Answer_TextArea,
            Answer_Submit,
            Answer_ResponderId,
            qgr.QuestionGroupResponse_AssessmentResponseId as Answer_AssessmentResponseId,
            Answer_ReviewerComment,
            cast(Answer_Id as varchar(10)) + '_' + cast(Question_QuestionGroupResponseId as varchar(10)) as AnswerResponse_PK
        from q
        join QuestionGroup qg on qg.QuestionGroup_ID = q.Question_QuestionGroupId
        join QuestionGroupResponse qgr on qgr.QuestionGroupResponse_ID = q.Question_QuestionGroupResponseId
        join a on a.Answer_QuestionId = q.Question_Id
        where Question_IsMultiSelectType = 0 and a.Answer_ComponentStr is not NULL and Question_Type in (1, 2, 5, 6, 10)
    ),
    qa_group_multi as (
        select 
            qa.Question_TenantId,
            qa.Question_Id,
            qa.Question_LastModificationTime,
            qa.Question_Order,
            qa.Question_IdRef,
            qa.Question_Name, 
            qa.Question_Description,
            qa.Question_AssessmentDomainId,
            qa.Question_Type,
            qa.Question_TypeCode,
            qa.Question_Weighting,
            qa.Question_HasConditionalLogic,
            qa.Question_HiddenInSurveyForConditional,
            qa.Question_QuestionGroupId,
            qa.Question_QuestionGroupResponseId,
            qa.Question_IsMandatory,
            qa.Question_DisplayDocumentUpload,
            qa.Answer_QuestionId,
            qa.Answer_Id,
            qa.Answer_LastModificationTime,
            qa.Answer_Compliance,
            qa.Answer_ComplianceCode,
            qa.Answer_ResponseCount,
            cast(qq.Question_Weighting * qq.QuestionOption_Rank as decimal(15, 2)) Answer_Score,
            qq.QuestionOption_RiskStatus Answer_RiskStatus,
            case
                qq.QuestionOption_RiskStatus
                when 0
                then 'No Risk'
                when 6
                then 'Very Low Risk'
                when 1
                then 'Low Risk'
                when 3
                then 'Medium Risk'
                when 4
                then 'High Risk'
                when 5
                then 'Very High Risk'
                when NULL 
                then 'Undefined'
                else 'Undefined'
            end as RiskStatusCode,
            case
                qq.QuestionOption_RiskStatus
                when 0
                then 0.0
                when 6
                then 1.0
                when 1
                then 2.0
                when 3
                then 3.0
                when 4
                then 4.0
                when 5
                then 5.0
                else cast(NULL as decimal(7, 2))
            end as RiskStatusCalc,
            sum(CAST(qq.Question_Weighting as bigint) * CAST(qq.QuestionOption_Rank as bigint)) over (
                partition by qq.Question_Id
            ) Question_MaxPossibleScore,
            cast(answer_kv. [key] + 1 as int) AnswerResponse_key,
            cast(answer_kv.value as nvarchar(2000)) as AnswerResponse_Value,
            qa.Answer_TextArea,
            Answer_Submit,
            Answer_ResponderId,
            Answer_AssessmentResponseId,
            Answer_ReviewerComment,
            concat(qa.Answer_Id, '_', Question_QuestionGroupResponseId, '_', answer_kv. [key]) AnswerResponse_PK
        from qa_multi_json qa outer apply openjson(qa.Answer_ResponseJson, '$.answer') as answer_kv
        left join qq on qa.Question_Id = qq.Question_Id and answer_kv.value = qq.QuestionOption_Value
        join QuestionGroup qg on qg.QuestionGroup_ID = qa.Question_QuestionGroupId
        join QuestionGroupResponse qgr on qgr.QuestionGroupResponse_ID = qa.Question_QuestionGroupResponseId
    ),
    qa as (
        select 'single' part, *
        from qa_single  -- Question with answer of type single answer
        union 
        select 'multi' part, *
        from qa_multi  -- Question with answer of type Multi Select Answer
        union 
        select 'freetext' part, *
        from qa_freetext  -- Question of freetext type
        union 
        select 'group single' part, *
        from qa_group_single  -- Question with no answer
        union 
        select 'group multi' part, *
        from qa_group_multi  -- Question with no answer
        union 
        select 'no answer' part, *
        from qa_no_answer  -- Question with no answer
    )
    ,
    final as (  -- multi row answers
        select 
            part,
            Question_TenantId Answer_TenantId,
            Question_Id,
            Question_Order,
            Question_IdRef,
            Question_Name, 
            Question_Description,
            Question_AssessmentDomainId,
            Question_Type,
            Question_TypeCode,
            Question_Weighting,
            Answer_Id,
           -- Answer_LastModificationTime,
            Answer_Compliance,
            Answer_ComplianceCode,
            Answer_ResponseCount,
            Answer_MaxPossibleScore,
            Answer_ResponderId,
            Answer_AssessmentResponseId,
            Answer_ReviewerComment,
            AnswerResponse_key,
            Question_HasConditionalLogic,
            Question_HiddenInSurveyForConditional,
            -- Actual Response is used in group by in count chart, cannot be NULL    
            cast(case
                when Question_HiddenInSurveyForConditional = 1
                then 'Blank'
                when Question_Type in (4, 9)
                then coalesce(AnswerResponse_Value, 'Blank')
                when AnswerResponse_Value is NULL
                then 'Blank'
                when Answer_ResponderId is not NULL
                then case when len(AnswerResponse_Value) = 0 then 'Blank' else AnswerResponse_Value end
            end as nvarchar(2000))
            as AnswerResponse_Value,
            -- Explanatory column from App
            cast(case
                when Question_HiddenInSurveyForConditional = 1
                then 'Blank because skip logic is applied'
                when Question_Type in (4, 9)
                then Answer_TextArea
                when Answer_ResponderId is not NULL
                then Answer_TextArea
                when Answer_ResponderId is NULL
                then 'Blank because Question is not responded to'
            end as nvarchar(4000)) Answer_TextArea,
            -- Question Status displayed in Drill Thru report
            case
                when Question_HiddenInSurveyForConditional = 1
                then 'Skip Logic Applied'
                when Answer_ResponderId is not NULL
                then 'Responded'
                when Answer_ResponderId is NULL
                then 'Not Answered'
            end Question_Status,
            -- Answer Score is a metric that is aggregated, NULL or zero logic follows spec
            case
                when Question_HiddenInSurveyForConditional = 1
                then NULL
                when Question_Type in (4, 9)
                then NULL
                when Answer_ResponderId is not NULL
                then Answer_Score
                when Answer_ResponderId is NULL
                then 0
            end Answer_Score,
            -- Risk Status is the raw risk score stored by the app and needs to be transformed to be used as a metric
            case
                when Question_HiddenInSurveyForConditional = 1
                then NULL
                when Question_Type in (4, 9)
                then NULL
                when Answer_ResponderId is not NULL
                then Answer_RiskStatus
                when Answer_ResponderId is NULL
                then 0
            end Answer_RiskStatus,
            -- Risk Rating Label is used in group by in count chart, cannot be NULL
            case
                when Question_HiddenInSurveyForConditional = 1
                then 'Skip Logic Applied'
                when Question_Type in (4, 9)
                then 'Not Risk Rated'
                when Answer_ResponderId is not NULL
                then Answer_RiskStatusCode
                when Answer_ResponderId is NULL
                then 'Not Answered'
            end Answer_RiskStatusCode,
            -- Risk Status used for metric used for aggregation
            -- when averaged, then need to round and mapped back to the Risk Label between 0 to 5, logic above
            case
                when Question_HiddenInSurveyForConditional = 1
                then NULL
                when Question_Type in (4, 9)
                then NULL
                when Answer_ResponderId is not NULL
                then Answer_RiskStatusCalc
                when Answer_ResponderId is NULL
                then 0.00
            end Answer_RiskStatusCalc,
            Question_QuestionGroupId,
            Question_QuestionGroupResponseId,
            Question_IsMandatory,
            Question_DisplayDocumentUpload,
            AnswerResponse_PK,
			case when coalesce(Answer_LastModificationTime , '2000-01-01 00:00:00') >= Question_LastModificationTime 
                then Answer_LastModificationTime  
                else Question_LastModificationTime end as QBA_QA_UpdateTime
            --max(GREATEST(Answer_LastModificationTime,Question_LastModificationTime))over(partition by Question_Id,Answer_Id) as QBA_QA_UpdateTime
        from qa
    )
    , answer_list as (
        select 
            f.Answer_TenantId,
            f.Question_AssessmentDomainId,
            f.Question_Id,
            STRING_AGG(f.AnswerResponse_Value, ', ') as AnswerResponseValue_List
        from final f
        group by 
            f.Answer_TenantId,
            f.Question_AssessmentDomainId,
            f.Question_Id
    )
    , main as (    
    select distinct f.part
            ,f.Answer_TenantId
            ,f.Question_Id
            ,f.Question_Order
            ,f.Question_IdRef
            ,f.Question_Name
            ,f.Question_Description
            ,f.Question_AssessmentDomainId
            ,f.Question_Type
            ,f.Question_TypeCode
            ,f.Question_Weighting
            ,f.Answer_Id
            ,f.Answer_Compliance
            ,f.Answer_ComplianceCode
            ,f.Answer_ResponseCount
            ,f.Answer_MaxPossibleScore
            ,f.Answer_ResponderId
            ,f.Answer_AssessmentResponseId
            ,f.Answer_ReviewerComment
            ,f.AnswerResponse_key
            ,f.Question_HasConditionalLogic
            ,f.Question_HiddenInSurveyForConditional
            ,f.AnswerResponse_Value
            ,f.Answer_TextArea
            ,f.Question_Status
            ,f.Answer_Score
            ,f.Answer_RiskStatus
            ,f.Answer_RiskStatusCode
            ,f.Answer_RiskStatusCalc
            ,f.Question_QuestionGroupId
            ,f.Question_QuestionGroupResponseId
            ,f.Question_IsMandatory
            ,f.Question_DisplayDocumentUpload
            ,f.AnswerResponse_PK
            ,f.QBA_QA_UpdateTime
            ,al.AnswerResponseValue_List
            
    from final f
    join answer_list al
    on al.Answer_TenantId = f.Answer_TenantId
    and al.Question_AssessmentDomainId = f.Question_AssessmentDomainId
    and al.Question_Id = f.Question_Id
    )

Select *
,rank()over(order by Question_Id,coalesce(AnswerResponse_PK,'')) as QBA_QuestionAnswer_pk 
 from main
{# where Answer_TenantId = 1384 #}