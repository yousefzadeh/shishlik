/*
    Assessment Response Linked Issue IDs 

    Assessment -> AssessmentDomain -> AssessmentDomainProvision -> AssessmentDomainProvisionIssue table (for each provision) - IssueId column
    Assessment -> AssessmentResponse -> AssessmentDomainProvisionIssue table (for assessment if response is created) - IssueId column
    Assessment -> AssessmentDomainProvisionIssue table (for assessment irrespective of response check) - IssueId column

    UNION ALL

    Assessment -> AssessmentDomain -> AssessmentDomainControl -> AssessmentDomainControlissue table (for each control) - IssueId column
    Assessment -> AssessmentResponse -> AssessmentDomainControlIssue table (for assessment if response is created) - IssueId column
    Assessment -> AssessmentDomainControlIssue table (for assessment irrespective of response check) - IssueId column
*/

with auth_issue as (
    --     Assessment -> AssessmentDomain -> AssessmentDomainProvision -> AssessmentDomainProvisionIssue table (for each provision) - IssueId column
        select distinct
            'Provision' Requirement_Type,
            AssessmentDomainProvision_Id Requirement_Id,
            AssessmentDomainProvision_TenantId Requirement_TenantId,
            AssessmentDomainProvisionIssue_AssessmentResponseId AssessmentResponse_Id,
            Issues_Id,
            Issues_IdRef + ': ' + Issues_Name as Issue_Text
        from {{ ref("vwAssessmentDomainProvision") }} adp
        join
            {{ ref("vwAssessmentDomainProvisionIssue") }} adpi
            on AssessmentDomainProvisionIssue_AssessmentDomainProvisionId = AssessmentDomainProvision_Id
        join {{ ref("vwIssues") }} i on i.Issues_Id = AssessmentDomainProvisionIssue_IssueId
    ),
    control_issue as (
    --     Assessment -> AssessmentDomain -> AssessmentDomainControl -> AssessmentDomainControlissue table (for each control) - IssueId column
        select distinct
            'Control' Requirement_Type,
            AssessmentDomainControl_Id Requirement_Id,
            AssessmentDomainControl_TenantId Requirement_TenantId,
            AssessmentDomainControlIssue_AssessmentResponseId AssessmentResponse_Id,
            Issues_Id,
            Issues_IdRef + ': ' + Issues_Name as Issue_Text
        from {{ ref("vwAssessmentDomainControl") }} adc
        join
            {{ ref("vwAssessmentDomainControlIssue") }} adci
            on AssessmentDomainControlIssue_AssessmentDomainControlId = AssessmentDomainControl_Id
        join {{ ref("vwIssues") }} i on i.Issues_Id = AssessmentDomainControlIssue_IssueId
    ),
    all_issues as (
        select *
        from auth_issue
        union all
        select *
        from control_issue
    )
select * from all_issues