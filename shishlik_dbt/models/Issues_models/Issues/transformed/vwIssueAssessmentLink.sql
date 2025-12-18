{{ config(materialized="view") }}

select
    'QBA' as AssessmentType,
    IssueAssessment_Id,
    IssueAssessment_TenantId,
    IssueAssessment_IssueId,
    IssueAssessment_AssessmentId,
    'Assessment Domain' as AssessmentDomainType,
    IssueAssessment_AssessmentDomainId,
    IssueAssessment_QuestionId
from {{ ref("vwIssueAssessment") }}

union all

select
    'RBA' as AssessmentType,
    AssessmentDomainProvisionIssue_Id,
    AssessmentDomainProvisionIssue_TenantId,
    AssessmentDomainProvisionIssue_IssueId,
    AssessmentDomainProvisionIssue_AssessmentId,
    'Provision Requirements' as AssessmentDomainType,
    AssessmentDomainProvisionIssue_AssessmentDomainProvisionId,
    AssessmentDomainProvisionIssue_AssessmentResponseId
from {{ ref("vwAssessmentDomainProvisionIssue") }}

union all

select
    'RBA' as AssessmentType,
    AssessmentDomainControlIssue_Id,
    AssessmentDomainControlIssue_TenantId,
    AssessmentDomainControlIssue_IssueId,
    AssessmentDomainControlIssue_AssessmentId,
    'Control Requirements' as AssessmentDomainType,
    AssessmentDomainControlIssue_AssessmentDomainControlId,
    AssessmentDomainControlIssue_AssessmentResponseId
from {{ ref("vwAssessmentDomainControlIssue") }}
