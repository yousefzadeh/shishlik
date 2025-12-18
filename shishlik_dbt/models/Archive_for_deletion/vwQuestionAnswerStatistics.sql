with
    detail as (
        select
            a.Assessment_ID,
            a.Assessment_Name,
            ad.AssessmentDomain_Order,
            ad.AssessmentDomain_ID,
            ad.AssessmentDomain_Name,
            qa.Question_Order,
            qa.Question_Name,
            qa.Answer_RadioCustom,
            qa.Answer_TextArea,
            qa.Answer_Score
        -- percent_rank() over (partition by ad.AssessmentDomain_Name order by qa.Answer_Score) percentile
        from {{ ref("vwAssessment") }} a
        inner join {{ ref("vwAssessmentDomain") }} ad on (a.Assessment_ID = ad.AssessmentDomain_AssessmentId)
        inner join {{ ref("vwQuestionAnswer") }} qa on (ad.AssessmentDomain_ID = qa.Question_AssessmentDomainId)
        inner join
            {{ ref("vwAssessment") }} a2
            on (a2.Assessment_ID = a.Assessment_CreatedFromTemplateId)
            and (a2.Assessment_IsTemplate = 1 and a2.Assessment_WorkFlowId = 0)
    -- WHERE (
    -- a.Assessment_IsTemplate = 0
    -- AND a.Assessment_WorkFlowId = 0
    -- )
    -- AND (
    -- a.Assessment_TenantId IN (1016, 1020, 1024, 1025, 1027, 1028, 1029, 1030, 1031, 1032, 1033, 1034, 1035, 1036,
    -- 1037, 1038, 1039, 1040, 1041, 1043, 1044, 1045, 1046, 1047, 1049, 1055, 271, 3, 563, 641, 646, 647, 657, 688,
    -- 697, 705, 711, 732, 737, 738, 739, 742, 791, 805, 829, 932, 993, 995, 996)
    -- )
    -- AND (
    -- (
    -- a2.Assessment_Name IN ('6clicks Pandemic Assessment')
    -- )
    --
    -- )
    ),
    -- , median1 as (
    -- SELECT top 10000 
    -- --	Assessment_ID,
    -- AssessmentDomain_Name, 
    -- Answer_Score, 
    -- PERCENTILE_CONT(0.5) 
    -- WITHIN GROUP (ORDER BY Answer_Score) 
    -- OVER (PARTITION BY AssessmentDomain_Name)
    -- AS Median_Score
    -- FROM detail
    -- where answer_score is not null
    -- --WHERE AssessmentDomain_Name = '5Finance & Legal'
    -- ORDER BY     
    -- --Assessment_ID,
    -- AssessmentDomain_Name, 
    -- Answer_Score
    -- )
    -- , median as (
    -- select distinct
    -- --	Assessment_ID,
    -- AssessmentDomain_Name, 
    -- Median_Score
    -- from median1
    -- )
    -- , mode1 as (
    -- select 
    -- Assessment_ID,
    -- AssessmentDomain_Name, 
    -- Answer_Score,
    -- count(*) frequency
    -- from detail 
    -- where Answer_Score is not null
    -- group by 
    -- Assessment_ID,
    -- AssessmentDomain_Name, 
    -- Answer_Score
    -- )
    -- , mode2 as (
    -- select 
    -- mode1.*,
    -- max(frequency) over (partition by AssessmentDomain_Name) max_frequency
    -- from mode1
    -- )
    -- , mode as (
    -- select
    -- Assessment_ID,
    -- AssessmentDomain_Name,
    -- avg(max_frequency) Mode_Score,
    -- count(*) Mode_Count
    -- from mode2
    -- where max_frequency = frequency
    -- group by 	
    -- Assessment_ID,
    -- AssessmentDomain_Name
    -- )
    min_max_avg as (
        select
            Assessment_ID,
            Assessment_Name,
            AssessmentDomain_ID,
            AssessmentDomain_Name,
            count(*) Count_Score,
            min(Answer_Score) Min_Score,
            max(Answer_Score) Max_Score,
            avg(Answer_Score) Avg_Score
        from detail
        group by Assessment_ID, Assessment_Name, AssessmentDomain_ID, AssessmentDomain_Name
    ),
    uni as (
        select distinct
            a.Assessment_ID,
            a.Assessment_Name,
            a.AssessmentDomain_ID,
            a.AssessmentDomain_Name,
            a.Count_Score,
            a.Min_Score,
            null Max_Score,
            null Avg_Score,
            -- null Median_Score,
            -- null Mode_Score,
            'Min' Name
        from min_max_avg a
        -- left join median on a.AssessmentDomain_Name = median.AssessmentDomain_Name
        -- left join mode on a.AssessmentDomain_Name = mode.AssessmentDomain_Name
        -- where Min_Score is not null and a.AssessmentDomain_Name = '5Finance & Legal'
        union all

        select distinct
            a.Assessment_ID,
            a.Assessment_Name,
            a.AssessmentDomain_ID,
            a.AssessmentDomain_Name,
            a.Count_Score,
            null Min_Score,
            a.Max_Score,
            null Avg_Score,
            -- null Median_Score,
            -- null Mode_Score,
            'Max' Name
        from min_max_avg a
        -- left join median on a.AssessmentDomain_Name = median.AssessmentDomain_Name
        -- left join mode on a.AssessmentDomain_Name = mode.AssessmentDomain_Name
        -- where Min_Score is not null and a.AssessmentDomain_Name = '5Finance & Legal'
        union all

        select distinct
            a.Assessment_ID,
            a.Assessment_Name,
            a.AssessmentDomain_ID,
            a.AssessmentDomain_Name,
            a.Count_Score,
            null Min_Score,
            null Max_Score,
            a.Avg_Score,
            -- null Median_Score,
            -- null Mode_Score,
            'Mean' Name
        from min_max_avg a
    -- left join median on a.AssessmentDomain_Name = median.AssessmentDomain_Name
    -- left join mode on a.AssessmentDomain_Name = mode.AssessmentDomain_Name
    -- where Min_Score is not null and a.AssessmentDomain_Name = '5Finance & Legal'
    -- union ALL 
    --
    -- select distinct
    -- a.Assessment_ID ,
    -- a.Assessment_Name ,
    -- a.AssessmentDomain_ID ,
    -- a.AssessmentDomain_Name ,
    -- a.Count_Score,
    -- null Min_Score,
    -- null Max_Score,
    -- null Avg_Score,
    -- median.Median_Score,
    -- null Mode_Score,
    -- 'Median' Name
    -- from min_max_avg a
    -- left join median on a.AssessmentDomain_Name = median.AssessmentDomain_Name
    -- left join mode on a.AssessmentDomain_Name = mode.AssessmentDomain_Name
    --
    -- where Min_Score is not null and a.AssessmentDomain_Name = '5Finance & Legal'
    --
    -- union ALL 
    --
    -- select distinct
    -- a.Assessment_ID ,
    -- a.Assessment_Name ,
    -- a.AssessmentDomain_ID ,
    -- a.AssessmentDomain_Name ,
    -- a.Count_Score,
    -- null Min_Score,
    -- null Max_Score,
    -- null Avg_Score,
    -- null Median_Score,
    -- mode.Mode_Score,
    -- 'Mode' Name
    -- from min_max_avg a
    -- left join median on a.AssessmentDomain_Name = median.AssessmentDomain_Name
    -- left join mode on a.AssessmentDomain_Name = mode.AssessmentDomain_Name
    --
    -- where Min_Score is not null and a.AssessmentDomain_Name = '5Finance & Legal'
    )

select *
from uni
