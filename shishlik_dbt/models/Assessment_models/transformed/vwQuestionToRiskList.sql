with
    risk as (
        select distinct 
            qr.AssessmentRisk_QuestionId as Question_Id, 
            r.Risk_IdRef, 
            r.Risk_Name, 
            qr.AssessmentRisk_UpdateTime
        from {{ ref("vwAssessmentRisk") }} qr
        join {{ ref("vwRisk") }} r on qr.AssessmentRisk_RiskId = r.Risk_Id
        where qr.AssessmentRisk_QuestionId is not null
    ),
    list as (
        select Question_Id, 
        string_agg(cast((Risk_IdRef + ': ' + Risk_Name) as varchar(max)), ', ') as RiskList,
        max(AssessmentRisk_UpdateTime) as QuestionToRiskList_UpdateTime
        from risk
        group by Question_Id
    )
select *
from list
