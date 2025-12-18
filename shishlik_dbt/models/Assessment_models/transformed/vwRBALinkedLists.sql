with
    auth_issue as (
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
    ),
    issues_list as (
        select
            Requirement_TenantId,
            Requirement_Type,
            Requirement_Id,
            AssessmentResponse_Id,
            string_agg(cast(Issue_Text as varchar(max)), ', ') IssuesList
        from all_issues
        group by Requirement_TenantId, Requirement_Type, Requirement_Id, AssessmentResponse_Id
    ),
    issue_actions_list as (
        select
            Requirement_TenantId,
            Requirement_Type,
            Requirement_Id,
            AssessmentResponse_Id,
            string_agg(cast(issueAction_IdRef + ': ' + IssueAction_Title as varchar(max)), ', ') as IssueActionsList
        from all_issues i
        join {{ ref("vwIssueAction") }} ia on Issues_Id = IssueAction_IssueId
        group by Requirement_TenantId, Requirement_Type, Requirement_Id, AssessmentResponse_Id
    ),
    auth_risk as (
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
    ),
    risks_list as (
        select
            Requirement_TenantId,
            Requirement_Type,
            Requirement_Id,
            AssessmentResponse_Id,
            string_agg(cast(Risk_Text as varchar(max)), ', ') RisksList
        from all_risk
        group by Requirement_TenantId, Requirement_Type, Requirement_Id, AssessmentResponse_Id
    ),
    auth_doc as (
        select distinct
            'Provision' Requirement_Type,
            AssessmentDomainProvision_Id Requirement_Id,
            AssessmentDomainProvision_TenantId Requirement_TenantId,
            AssessmentDomainProvisionResponseDocument_AssessmentResponseId AssessmentResponse_Id,
            AssessmentDomainProvisionResponseDocument_Id,
            AssessmentDomainProvisionResponseDocument_DisplayFileName as Attachment_Text
        from {{ ref("vwAssessmentDomainProvision") }} adp
        join
            {{ ref("vwAssessmentDomainProvisionResponseDocument") }} adprd
            on AssessmentDomainProvisionResponseDocument_AssessmentDomainProvisionId = AssessmentDomainProvision_Id
    ),
    control_doc as (
        select distinct
            'Control' Requirement_Type,
            AssessmentDomainControl_Id Requirement_Id,
            AssessmentDomainControl_TenantId Requirement_TenantId,
            AssessmentDomainControlResponseDocument_AssessmentResponseId AssessmentResponse_Id,
            AssessmentDomainControlResponseDocument_Id,
            AssessmentDomainControlResponseDocument_DisplayFileName as Attachment_Text
        from {{ ref("vwAssessmentDomainControl") }} adc
        join
            {{ ref("vwAssessmentDomainControlResponseDocument") }} adcrd
            on AssessmentDomainControlResponseDocument_AssessmentDomainControlId = AssessmentDomainControl_Id
    ),
    all_doc as (
        select *
        from auth_doc
        union all
        select *
        from control_doc
    ),
    docs_list as (
        select
            Requirement_TenantId,
            Requirement_Type,
            Requirement_Id,
            AssessmentResponse_Id,
            string_agg(cast(Attachment_Text as varchar(max)), ', ') AttachmentsList
        from all_doc
        group by Requirement_TenantId, Requirement_Type, Requirement_Id, AssessmentResponse_Id
    ),
    final as (
        select
            req.Requirement_TenantId,
            req.Requirement_Type,
            req.Requirement_Id,
            -- Link to AssessmentResponse_Id to only show the issues, actions, risks, and attachments only responded assessment response
            coalesce(i.AssessmentResponse_Id, ia.AssessmentResponse_Id, ir.AssessmentResponse_Id, d.AssessmentResponse_Id) AssessmentResponse_Id,
            i.IssuesList,
            ia.IssueActionsList,
            ir.RisksList,
            d.AttachmentsList
        from {{ ref("vwRBARequirement") }} req
        left join
            issues_list i
            on req.Requirement_TenantId = i.Requirement_TenantId
            and req.Requirement_Type = i.Requirement_Type
            and req.Requirement_Id = i.Requirement_Id
        left join
            issue_actions_list ia
            on i.Requirement_TenantId = ia.Requirement_TenantId
            and i.Requirement_Type = ia.Requirement_Type
            and i.Requirement_Id = ia.Requirement_Id
        left join
            risks_list ir
            on i.Requirement_TenantId = ir.Requirement_TenantId
            and i.Requirement_Type = ir.Requirement_Type
            and i.Requirement_Id = ir.Requirement_Id
        left join
            docs_list d
            on i.Requirement_TenantId = d.Requirement_TenantId
            and i.Requirement_Type = d.Requirement_Type
            and i.Requirement_Id = d.Requirement_Id
    )
select *
from final
