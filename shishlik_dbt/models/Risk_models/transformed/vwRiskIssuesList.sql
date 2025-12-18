with
    issues_list as (
        -- issuerisk table is many to many of risk and issues
        select
            ir.IssueRisk_RiskId,
            STRING_AGG(cast(i.Issues_IdRef_New as varchar(12))+ ': ' + i.Issues_Name, ', ') as IssuesList
        from {{ ref("vwIssueRisk") }} ir
        inner join {{ ref("vwIssues") }} i on ir.IssueRisk_issueId = i.Issues_Id
        group by ir.IssueRisk_RiskId
    )
select *
from issues_list
