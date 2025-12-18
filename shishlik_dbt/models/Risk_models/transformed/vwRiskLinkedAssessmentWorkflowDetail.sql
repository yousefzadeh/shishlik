-- Risk > AssessmentRisk > Assessment (Workflow = Question) > Question
-- Risk > AssessmentRisk > Assessment (Workflow = Requirement) > AssessmentDomain > AssessmentDomainProvision
-- Risk > AssessmentRisk > Assessment (Workflow = Requirement) > AssessmentDomain > AssessmentDomainControl
with
    RiskAssessmentQuestion as (
        select
            r.Risk_TenantId,
            r.Risk_Id,
            a.Assessment_ID,
            'Question' Item,
            q.Question_Id ItemId,
            q.Question_IdRef ItemReferenceId,
            q.Question_Name ItemName
        from {{ ref("vwRisk") }} r
        join
            {{ ref("vwAssessmentRisk") }} ar
            on ar.AssessmentRisk_RiskId = r.Risk_Id
            and ar.AssessmentRisk_TenantId = r.Risk_TenantId
        join {{ ref("vwAssessmentDomain") }} ad on ar.AssessmentRisk_AssessmentId = ad.AssessmentDomain_ID
        join
            {{ ref("vwAssessment") }} a
            on ar.AssessmentRisk_AssessmentId = a.Assessment_ID
            and ar.AssessmentRisk_TenantId = a.Assessment_TenantId
        join
            {{ ref("vwQuestion") }} q
            on ar.AssessmentRisk_QuestionId = q.Question_Id
            and ar.AssessmentRisk_TenantId = q.Question_TenantId
    ),
    RiskAssessmentRBAProvision as (
        select
            r.Risk_TenantId,
            r.Risk_Id,
            adpr.AssessmentDomainProvisionRisk_AssessmentId Assessment_ID,
            'Provision Requirement' Item,
            ap.AuthorityProvision_Id ItemId,
            ap.AuthorityProvision_ReferenceId ItemReferenceId,
            ap.AuthorityProvision_Name ItemName
        from {{ ref("vwRisk") }} r
        join
            {{ ref("vwAssessmentDomainProvisionRisk") }} adpr
            on r.Risk_Id = adpr.AssessmentDomainProvisionRisk_RiskId
            and r.Risk_TenantId = adpr.AssessmentDomainProvisionRisk_TenantId
        join
            {{ ref("vwAssessmentDomainProvision") }} adp
            on adp.AssessmentDomainProvision_Id = adpr.AssessmentDomainProvisionRisk_AssessmentDomainProvisionId
        join
            {{ ref("vwDirectAuthorityProvision") }} ap
            on adp.AssessmentDomainProvision_AuthorityProvisionId = ap.AuthorityProvision_Id
            and adp.AssessmentDomainProvision_TenantId = ap.Tenant_Id
    ),
    RiskAssessmentRBAControl as (
        select
            r.Risk_TenantId,
            r.Risk_Id,
            adcr.AssessmentDomainControlRisk_AssessmentId Assessment_ID,
            'Control Requirement' Item,
            c.Controls_Id ItemId,
            c.Controls_Reference ItemReferenceId,
            c.Controls_Name ItemName
        from {{ ref("vwRisk") }} r
        join
            {{ ref("vwAssessmentDomainControlRisk") }} adcr
            on r.Risk_Id = adcr.AssessmentDomainControlRisk_RiskId
            and r.Risk_TenantId = adcr.AssessmentDomainControlRisk_TenantId
        join
            {{ ref("vwAssessmentDomainControl") }} adc
            on adcr.AssessmentDomainControlRisk_AssessmentDomainControlId = adc.AssessmentDomainControl_Id
            and adcr.AssessmentDomainControlRisk_TenantId = adc.AssessmentDomainControl_TenantId
        join
            {{ ref("vwControls") }} c
            on adc.AssessmentDomainControl_ControlsId = c.Controls_Id
            and adc.AssessmentDomainControl_TenantId = c.Controls_TenantId
    ),
    final as (
        select *
        from RiskAssessmentQuestion
        union all
        select *
        from RiskAssessmentRBAProvision
        union all
        select *
        from RiskAssessmentRBAControl
    )
select *
from final
