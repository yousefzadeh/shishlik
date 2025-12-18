with
    total as (
        select
            -- AssessmentDomain_Id,
            T.AssessmentDomain_Name, min(Total_Score) Min, max(Total_Score) Max, avg(Total_Score) Mean
        from
            (

                select
                    ad.AssessmentDomain_Name,
                    ad.AssessmentDomain_Id,
                    ad.AssessmentDomain_AssessmentId,
                    sum(Answer_Score) Total_Score,
                    avg(Answer_Score) Avg_Score

                from {{ ref("vwQuestionAnswer") }} qaj
                join {{ ref("vwAssessmentDomain") }} ad on ad.AssessmentDomain_Id = qaj.Question_AssessmentDomainId
                -- where 
                -- AssessmentDomain_Name = 'Finance & Legal'
                -- and AssessmentDomain_AssessmentId in (63, 620, 808)
                group by ad.AssessmentDomain_AssessmentId, ad.AssessmentDomain_Id, ad.AssessmentDomain_Name

            ) as T
        group by
            -- AssessmentDomain_Id,
            AssessmentDomain_Name
    ),
    average as (
        select AssessmentDomain_Name, min(Avg_Score) Min, max(Avg_Score) Max, avg(Avg_Score) Mean
        from
            (

                select
                    ad.AssessmentDomain_Name,
                    -- ad.AssessmentDomain_Id,
                    ad.AssessmentDomain_AssessmentId,
                    sum(Answer_Score) Total_Score,
                    avg(Answer_Score) Avg_Score

                from {{ ref("vwQuestionAnswer") }} qaj
                join {{ ref("vwAssessmentDomain") }} ad on ad.AssessmentDomain_Id = qaj.Question_AssessmentDomainId
                -- where 
                -- AssessmentDomain_Name = 'Finance & Legal'
                -- and AssessmentDomain_AssessmentId in (63, 620, 808)
                group by
                    ad.AssessmentDomain_AssessmentId,
                    -- ad.AssessmentDomain_Id,
                    ad.AssessmentDomain_Name

            ) as A
        group by AssessmentDomain_Name
    ),
    Total_Uni as (
        select AssessmentDomain_Name, t.Min, null Max, null Mean, 'Min' Roll_Up
        --
        from total t

        union all

        select AssessmentDomain_Name, null Min, t.Max, null Mean, 'Max' Roll_Up

        from total t

        union all

        select AssessmentDomain_Name, null Min, null Max, t.Mean, 'Mean' Roll_Up

        from total t
    ),
    Average_Uni as (
        select AssessmentDomain_Name, a.Min, null Max, null Mean, 'Min' Roll_Up
        --
        from average a

        union all

        select AssessmentDomain_Name, null Min, a.Max, null Mean, 'Max' Roll_Up

        from average a

        union all

        select AssessmentDomain_Name, null Min, null Max, a.Mean, 'Mean' Roll_Up

        from average a
    ),
    ass_domain as (select ad.AssessmentDomain_ID, ad.AssessmentDomain_Name from {{ ref("vwAssessmentDomain") }} ad),
    meth as (
        select
            'Total' Methodology,
            ad.AssessmentDomain_ID,  -- PK
            tu.*

        from Total_Uni tu
        join ass_domain ad on ad.AssessmentDomain_Name = tu.AssessmentDomain_Name

        union all

        select
            'Average' Methodology,
            ad.AssessmentDomain_ID,  -- PK
            au.*

        from Average_Uni au
        join ass_domain ad on ad.AssessmentDomain_Name = au.AssessmentDomain_Name

    )

select m.*, a.Answer_RadioCustom, qtj.Tags_Name
from meth m
left join {{ ref("vwQuestion") }} q on q.Question_AssessmentDomainId = m.AssessmentDomain_ID
left join {{ ref("vwAnswer") }} a on a.Answer_QuestionId = q.Question_ID
left join {{ ref("vwQuestionTagsJoined") }} qtj on qtj.QuestionTags_QuestionId = q.Question_ID
