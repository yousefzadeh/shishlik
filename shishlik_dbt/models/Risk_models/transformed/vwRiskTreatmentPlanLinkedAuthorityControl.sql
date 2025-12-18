-- Risk > AssessmentRisk > Assessment (Workflow = Question) > Question
-- Risk > AssessmentRisk > Assessment (Workflow = Requirement) > AssessmentDomain > AssessmentDomainProvision
-- Risk > AssessmentRisk > Assessment (Workflow = Requirement) > AssessmentDomain > AssessmentDomainControl
with
    RiskTreatmentPlanProvision as (
        select
            rtp.RiskTreatmentPlan_TenantId,
            rtp.RiskTreatmentPlan_Id,
            'Provision' Item,
            ap.AuthorityProvision_Id ItemId,
            ap.AuthorityProvision_ReferenceId ItemReferenceId,
            ap.AuthorityProvision_Name ItemName
        from {{ ref("vwRiskTreatmentPlan") }} rtp
        join
            {{ ref("vwRiskTreatmentPlanProvision") }} rtpp
            on rtp.RiskTreatmentPlan_Id = rtpp.RiskTreatmentPlanProvision_RiskTreatmentPlanId
            and rtp.RiskTreatmentPlan_TenantId = rtpp.RiskTreatmentPlanProvision_TenantId
        join
            {{ ref("vwDirectAuthorityProvision") }} ap
            on rtpp.RiskTreatmentPlanProvision_AuthorityProvisionId = ap.AuthorityProvision_Id
            and rtpp.RiskTreatmentPlanProvision_TenantId = ap.Tenant_Id
    ),
    RiskTreatmentPlanAuthority as (
        select
            rtp.RiskTreatmentPlan_TenantId,
            rtp.RiskTreatmentPlan_Id,
            'Authority' Item,
            auth.Authority_Id ItemId,
            '#' + cast(auth.Authority_Id as varchar) ItemReferenceId,
            auth.Authority_Name ItemName
        from {{ ref("vwRiskTreatmentPlan") }} rtp
        join
            {{ ref("vwRiskTreatmentPlanProvision") }} rtpp
            on rtp.RiskTreatmentPlan_Id = rtpp.RiskTreatmentPlanProvision_RiskTreatmentPlanId
            and rtp.RiskTreatmentPlan_TenantId = rtpp.RiskTreatmentPlanProvision_TenantId
        join
            {{ ref("vwDirectAuthorityProvision") }} ap
            on rtpp.RiskTreatmentPlanProvision_AuthorityProvisionId = ap.AuthorityProvision_Id
            and rtpp.RiskTreatmentPlanProvision_TenantId = ap.Tenant_Id
        join
            {{ ref("vwDirectAuthority") }} auth on ap.Authority_Id = auth.Authority_Id and ap.Tenant_Id = auth.Tenant_Id
    ),
    RiskTreatmentPlanControl as (
        select
            rtp.RiskTreatmentPlan_TenantId,
            rtp.RiskTreatmentPlan_Id,
            'Control' Item,
            c.Controls_Id ItemId,
            c.Controls_Reference ItemReferenceId,
            c.Controls_Name ItemName
        from {{ ref("vwRiskTreatmentPlan") }} rtp
        join
            {{ ref("vwRiskTreatmentPlanControl") }} rtpc
            on rtp.RiskTreatmentPlan_Id = rtpc.RiskTreatmentPlanControl_RiskTreatmentPlanId
            and rtp.RiskTreatmentPlan_TenantId = rtpc.RiskTreatmentPlanControl_TenantId
        join
            {{ ref("vwControls") }} c
            on rtpc.RiskTreatmentPlanControl_ControlId = c.Controls_Id
            and rtpc.RiskTreatmentPlanControl_TenantId = c.Controls_TenantId
    ),
    RiskTreatmentPlanControlSet as (
        select
            rtp.RiskTreatmentPlan_TenantId,
            rtp.RiskTreatmentPlan_Id,
            'ControlSet' Item,
            p.Policy_Id ItemId,
            '#' + cast(p.Policy_Id as varchar) ItemReferenceId,
            p.Policy_Name ItemName
        from {{ ref("vwRiskTreatmentPlan") }} rtp
        join
            {{ ref("vwRiskTreatmentPlanPolicy") }} rtpp
            on rtp.RiskTreatmentPlan_Id = rtpp.RiskTreatmentPlanPolicy_RiskTreatmentPlanId
            and rtp.RiskTreatmentPlan_TenantId = rtpp.RiskTreatmentPlanPolicy_TenantId
        join
            {{ ref("vwPolicy") }} p
            on rtpp.RiskTreatmentPlanPolicy_PolicyId = p.Policy_Id
            and rtpp.RiskTreatmentPlanPolicy_TenantId = p.Policy_TenantId
    ),
    final as (
        select *
        from RiskTreatmentPlanProvision
        union all
        select *
        from RiskTreatmentPlanAuthority
        union all
        select *
        from RiskTreatmentPlanControl
        union all
        select *
        from RiskTreatmentPlanControlSet
    )
select *
from final
