{{ config(materialized="view") }}

select i.Issues_Id, i.Issues_TenantId, i.Issues_Name, STRING_AGG(a.Assessment_Name, ',') Issues_ConcatAssessmnent
from {{ ref("vwIssues") }} i
inner join
    {{ ref("vwIssueAssessment") }} ia
    on i.Issues_Id = ia.IssueAssessment_IssueId
    and i.Issues_TenantId = ia.IssueAssessment_TenantId
inner join {{ ref("vwAssessment") }} a on ia.IssueAssessment_AssessmentId = a.Assessment_ID
where i.Issues_Name = 'New issue 1'
group by i.Issues_id, i.Issues_TenantId, i.Issues_Name
