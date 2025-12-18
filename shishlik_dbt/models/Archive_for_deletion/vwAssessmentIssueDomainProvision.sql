{{ config(materialized="view") }}

select
    adpi.AssessmentDomainProvisionIssue_AssessmentDomainProvisionId,
    STRING_AGG(i.Issues_Name, ',') as Assessment_Issue_Name
from {{ ref("vwAssessmentDomainProvisionIssue") }} adpi
inner join {{ ref("vwIssues") }} i on i.Issues_Id = adpi.AssessmentDomainProvisionIssue_IssueId
group by adpi.AssessmentDomainProvisionIssue_AssessmentDomainProvisionId
