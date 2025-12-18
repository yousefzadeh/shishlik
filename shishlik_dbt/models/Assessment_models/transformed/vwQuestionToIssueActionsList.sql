with
    action as (
        select distinct 
            iass.IssueAssessment_QuestionId as Question_Id, 
            ia.IssueAction_IdRef, 
            ia.IssueAction_Title,
            iass.IssueAssessment_UpdateTime
        from {{ ref("vwIssues") }} i
        join {{ ref("vwIssueAssessment") }} iass on iass.IssueAssessment_IssueId = i.Issues_Id
        join {{ ref("vwIssueAction") }} ia on iass.IssueAssessment_IssueId = ia.IssueAction_IssueId
        where iass.IssueAssessment_QuestionId is not null        
    ),
    list as (
        select Question_Id, 
            string_agg( cast((IssueAction_IdRef + ': ' + IssueAction_Title) as varchar(max)), ', ') IssueActionsList,
            max(IssueAssessment_UpdateTime) as QIssueAssessment_UpdateTime
        from action 
        group by Question_Id
    )

select *
from list
