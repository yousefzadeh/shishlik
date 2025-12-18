/*
    Assessment Response Linked Issue IDs 

    Assessment -> AssessmentDomain -> AssessmentDomainProvision -> AssessmentDomainProvisionRisk table (for each provision) -> RiskId column
    Assessment -> AssessmentResponse -> AssessmentDomainProvisionRisk table (for assessment if response is created) > RiskId column
    Assessment -> AssessmentDomainProvisionRisk table (for assessment irrespective of response check) > RiskId column

    UNION ALL

    Assessment -> AssessmentDomain -> AssessmentDomainControl -> AssessmentDomainControlRisk table (for each control) -> RiskId column
    Assessment -> AssessmentResponse -> AssessmentDomainControlRisk table (for assessment if response is created) > RiskId column
    Assessment -> AssessmentDomainControlRisk table (for assessment irrespective of response check) > RiskId column

*/

with auth_risk as (
        select distinct
            'Provision' Requirement_Type,
            AssessmentDomainProvision_Id Requirement_Id,
            AssessmentDomainProvision_TenantId Requirement_TenantId,
            AssessmentDomainProvisionRisk_AssessmentResponseId AssessmentResponse_Id,
            Risk_Id,
            Risk_IdRef + ': ' + Risk_Name as Risk_Text
        from {{ ref("vwAssessmentDomainProvision") }} adp
        join
            {{ ref("vwAssessmentDomainProvisionRisk") }} adpr
            on AssessmentDomainProvisionRisk_AssessmentDomainProvisionId = AssessmentDomainProvision_Id
        join {{ ref("vwRisk") }} r on r.Risk_Id = AssessmentDomainProvisionRisk_RiskId
    ),
    control_risk as (
        select distinct
            'Control' Requirement_Type,
            AssessmentDomainControl_Id Requirement_Id,
            AssessmentDomainControl_TenantId Requirement_TenantId,
            AssessmentDomainControlRisk_AssessmentResponseId AssessmentResponse_Id,
            Risk_Id,
            Risk_IdRef + ': ' + Risk_Name as Risk_Text
        from {{ ref("vwAssessmentDomainControl") }} adc
        join
            {{ ref("vwAssessmentDomainControlRisk") }} adcr
            on AssessmentDomainControlRisk_AssessmentDomainControlId = AssessmentDomainControl_Id
        join {{ ref("vwRisk") }} r on r.Risk_Id = AssessmentDomainControlRisk_RiskId
    ),
    all_risk as (
        select *
        from auth_risk
        union all
        select *
        from control_risk
    )
select * from all_risk