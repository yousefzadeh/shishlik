{#
    Refered by Question Based Assessment Answer Details Risk Radar
    Join columns 
    Question_Id 
    Question_TenantId
    Question_AssessmentDomainId
    Question_IsTargetResponse

    Select Column
    Question_TargetRiskStatus
#}

with Question  as (
        select
            [Id],
            cast([Name] as nvarchar(500))[Name],
            [AssessmentDomainId],
            [TenantId],
            [Weighting],
            case when [ComponentStr]  = 'null' then NULL else [ComponentStr] end as [ComponentStr1],
            cast([IdRef] as nvarchar(100))[IdRef],
            cast(CONCAT([AssessmentDomainId], [IdRef], [Code]) as nvarchar(4000)) as PK,  -- Business Key
            coalesce(q.LastModificationTime, q.CreationTime) as QTR_UpdateTime
        from
            {{ source("assessment_models", "Question") }} q
            {{ system_remove_IsDeleted() }}
    ),
    base as (
        select
            [Id],
            [Name],
            [AssessmentDomainId],
            [TenantId],
            [Weighting],
            coalesce(json_value(js_kv. [value], '$.rank'), 1) OptionRank,
            coalesce(json_value(js_kv. [value], '$.value'), '') OptionValue,
            cast(json_value(js_kv. [value], '$.riskStatus') as decimal(9, 2)) TargetRiskStatus,
            case when json_value(js_kv. [value], '$.isTargetResponse') = 'true' then 1 else 0 end IsTargetResponse,
            [IdRef],
            q.PK,  -- Business Key
            q.QTR_UpdateTime
        from Question q
            cross apply openjson(q.ComponentStr1, '$.components.radiocustom.values') js_kv
    ),
    main as (
        select distinct
            [ID] as [Question_ID], 
            [Name] as [Question_Name],
            [AssessmentDomainId] as [Question_AssessmentDomainId], 
            [TenantId] as [Question_TenantId], 
            [Weighting] as [Question_Weighting],
            [IdRef] as [Question_IdRef],
            [IsTargetResponse] as [Question_IsTargetResponse], 
            [OptionRank] as [Question_OptionRank],
            OptionRank * Weighting as [Question_TargetScore],
            OptionRank * Weighting as [Target_Weight],
            OptionValue as [Question_TargetResponse],
            [TargetRiskStatus] as [Question_TargetRiskStatus],
            case
                TargetRiskStatus
                when 0.0
                then 'No Risk'
                when 6.0
                then 'Very Low Risk'
                when 1.0
                then 'Low Risk'
                when 3.0
                then 'Medium Risk'
                when 4.0
                then 'High Risk'
                when 5.0
                then 'Very High Risk'
                else 'Undefined'
            end as Question_TargetRiskStatusCode,
            case
                TargetRiskStatus
                when 0.0
                then 0.0
                when 6.0
                then 1.0
                when 1.0
                then 2.0
                when 3.0
                then 3.0
                when 4.0
                then 4.0
                when 5.0
                then 5.0
                else NULL
            end as Question_TargetRiskStatusCalc,
            [PK] as [Question_PK],
            [QTR_UpdateTime]
        from base
        where [IsTargetResponse] = 1
    )
select *
from
    main q
{#
-- Test case in SPINUP
where q.Question_ID in 
    (
        select q.Id 
        from Assessment a
        join AssessmentDomain ad on a.Id = ad.AssessmentId
        join Question q on ad.Id = q.AssessmentDomainId
        where a.TenantId = 1384 -- a.Id = 34639
    )
#}