with
    IssueAssessment as (
        select {{ system_fields_macro() }},
            [TenantId], 
            [IssueId], 
            [AssessmentId], 
            [AssessmentDomainId], 
            [QuestionId],
            cast(coalesce([LastModificationTime],[CreationTime]) as datetime2) as UpdateTime
        from {{ source("issue_models", "IssueAssessment") }} {{ system_remove_IsDeleted() }}
    ),
    issue as (
        select distinct 
        iass.QuestionId as Question_Id, 
        i.Issues_IdRef, 
        i.Issues_Name,
        IIF(i.Issues_UpdatedTime>iass.UpdateTime, Issues_UpdatedTime, iass.UpdateTime) as max_UpdateTime
        from IssueAssessment iass
        join {{ ref("vwIssues") }} i on iass.IssueId = i.Issues_Id
        where iass.QuestionId is not null
    ),
    list as (
        select Question_Id, 
        string_agg(cast( (Issues_IdRef + ': ' + Issues_Name) as varchar(max)), ', ') as IssuesList,
        max(max_UpdateTime) as QuestionToIssuesList_UpdateTime
        from issue 
        group by Question_Id
    )
select *
from list
