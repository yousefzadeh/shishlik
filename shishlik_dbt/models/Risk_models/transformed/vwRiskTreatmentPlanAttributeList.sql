{{ config(materialized="view") }}

{# /*
 * Risk Title
 * Risk Description
 * Risk Tags
 * Treatment Decision
 * Treatment Status
 * Treatment Plan Status
 * Treatment Plan Name
 * Treatment Plan Description
 * Planned Controls
 * Planned Provision
 * Treatment plan owner
 * Treatment plan due date
 */ #}
with
    risk as (
        select Risk_Id, Risk_TenantId, Risk_Name Risk_Title, Risk_Description
        {# Risk_Status,
	Risk_StatusCode,
	Risk_IdRef,
	Risk_AbstractRiskId,
	Risk_RiskReviewId,
	Risk_RiskCategoryId,
	Risk_CommonCause,
	Risk_LikelyImpact,
	Risk_IdentifiedBy,
	Risk_FavouriteRiskAssessmentId,
	Risk_IsCurrent #}
        from {{ ref("vwRisk") }}
    ),
    rtag as (
        {#     
	-- RiskTag_Tags
    -- one row for each RiskId 
    #}
        select rt.RiskTag_RiskId, STRING_AGG(t.Tags_Name, ', ') as RiskTagList
        from {{ ref("vwRiskTag") }} rt
        inner join {{ ref("vwTags") }} t on rt.RiskTag_TagId = t.Tags_ID
        group by rt.RiskTag_RiskId
    ),
    trt as (
        {# one row for each Risk treatment #}
        select
            r.Risk_Id,
            r.Risk_TenantId,
            r.Risk_TreatmentStatusCode RiskTreatment_StatusCode,
            r.Risk_TreatmentDecisionId RiskTreatment_DecisionId,
            sl.StatusLists_Name RiskTreatment_DecisionCode,
            sl.StatusLists_Reference,
            sl.StatusLists_IsClosedActionStatus,
            sl.StatusLists_StatusOrder
        from {{ ref("vwRisk") }} r
        join {{ ref("vwStatusLists") }} sl on r.Risk_TreatmentDecisionId = sl.StatusLists_Id
    ),
    trt_plan as (
        {# many to many #}
        select
            rtp.RiskTreatmentPlan_Id,
            rtpa.RiskTreatmentPlanAssociation_Id,
            rt.RiskTreatment_Id,
            r.Risk_Id RiskTreatment_RiskId,
            rtp.RiskTreatmentPlan_TenantId,
            rtp.RiskTreatmentPlan_TreatmentDescription,
            rtp.RiskTreatmentPlan_TreatmentDate,
            rtp.RiskTreatmentPlan_TreatmentName,
            rtp.RiskTreatmentPlan_IsDeprecated,
            rtp.RiskTreatmentPlan_DueDateStatus,
            rtp.RiskTreatmentPlan_Status,
            case
                when rtp.RiskTreatmentPlan_Status = 0
                then 'New'
                when rtp.RiskTreatmentPlan_Status = 1
                then 'Completed'
                when rtp.RiskTreatmentPlan_Status = 3
                then 'In-Progress'
            end RiskTreatmentPlan_StatusCode
        from {{ ref("vwRisk") }} r
        left join {{ ref("vwRiskTreatment") }} rt
        on rt.RiskTreatment_RiskId = r.Risk_Id
        left join {{ ref("vwRiskTreatmentPlanAssociation") }} rtpa
        on rtpa.RiskTreatmentPlanAssociation_RiskId = r.Risk_Id
        left join {{ ref("vwRiskTreatmentPlan") }} rtp
        on rtp.RiskTreatmentPlan_Id = rtpa.RiskTreatmentPlanAssociation_RiskTreatmentPlanId
--        where r.Risk_TenantId = 1384
--        and r.Risk_Id = 4177
    ),
    [owner] as (
        select
            rtpo.RiskTreatmentPlanOwner_TenantId,
            rtpo.RiskTreatmentPlanOwner_RiskTreatmentPlanId,
            STRING_AGG(coalesce(u.AbpUsers_FullName, aou.AbpOrganizationUnits_DisplayName), ', ') as OwnerList

        from {{ ref("vwRiskTreatmentPlanOwner") }} rtpo
        left join {{ ref("vwAbpUser") }} u on rtpo.RiskTreatmentPlanOwner_UserId = u.AbpUsers_Id
        left join
            {{ ref("vwAbpOrganizationUnits") }} aou
            on rtpo.RiskTreatmentPlanOwner_OrganizationUnitId = aou.AbpOrganizationUnits_Id
            and rtpo.RiskTreatmentPlanOwner_TenantId = aou.AbpOrganizationUnits_TenantId
        group by rtpo.RiskTreatmentPlanOwner_TenantId, rtpo.RiskTreatmentPlanOwner_RiskTreatmentPlanId
    ),
    ctrl as (
        {# 
    Controls
	one row per RiskTreatmentPlanId 
    #}
        select
            rtpc.RiskTreatmentPlanControl_TenantId,
            rtpc.RiskTreatmentPlanControl_RiskTreatmentPlanId,
            STRING_AGG(c.Controls_Name, ', ') as ControlList
        from {{ ref("vwRiskTreatmentPlanControl") }} rtpc
        join
            {{ ref("vwControls") }} c
            on rtpc.RiskTreatmentPlanControl_ControlId = c.Controls_Id
            and rtpc.RiskTreatmentPlanControl_TenantId = c.Controls_TenantId
        join {{ ref("vwPolicyDomain") }} pd on pd.PolicyDomain_Id = c.Controls_PolicyDomainId
        join {{ ref("vwPolicy") }} p on p.Policy_Id = pd.PolicyDomain_PolicyId and p.Policy_Status != 100
        group by rtpc.RiskTreatmentPlanControl_TenantId, rtpc.RiskTreatmentPlanControl_RiskTreatmentPlanId

    ),
    trt_ctrl as (

        select
            rtpc.RiskTreatmentPlanControl_TenantId,
            rtpc.RiskTreatmentPlanControl_RiskTreatmentPlanId,
            (c.Controls_Reference + ' ' + cast(c.Controls_Name as nvarchar(max))) TreatmentPlan_Controls
        -- STRING_AGG(c.Controls_Name,', ') as ControlList
        from {{ ref("vwRiskTreatmentPlanControl") }} rtpc
        join
            {{ ref("vwControls") }} c
            on rtpc.RiskTreatmentPlanControl_ControlId = c.Controls_Id
            and rtpc.RiskTreatmentPlanControl_TenantId = c.Controls_TenantId
        join {{ ref("vwPolicyDomain") }} pd on pd.PolicyDomain_Id = c.Controls_PolicyDomainId
        join {{ ref("vwPolicy") }} p on p.Policy_Id = pd.PolicyDomain_PolicyId and p.Policy_Status != 100
    -- group by 
    -- rtpc.RiskTreatmentPlanControl_TenantId,
    -- rtpc.RiskTreatmentPlanControl_RiskTreatmentPlanId
    ),
    auth_prov as (
        select
            rtpp.RiskTreatmentPlanProvision_TenantId,
            rtpp.RiskTreatmentPlanProvision_RiskTreatmentPlanId,
            a.Authority_Name,
            ap.AuthorityProvision_Name
        from {{ ref("vwRiskTreatmentPlanProvision") }} rtpp
        inner join
            {{ ref("vwAuthorityProvision") }} ap
            on rtpp.RiskTreatmentPlanProvision_AuthorityProvisionId = ap.AuthorityProvision_Id
        inner join {{ ref("vwAuthority") }} a on ap.AuthorityProvision_AuthorityId = a.Authority_Id
    ),
    prov_distinct as (
        select distinct
            RiskTreatmentPlanProvision_TenantId, RiskTreatmentPlanProvision_RiskTreatmentPlanId, AuthorityProvision_Name
        from auth_prov
    ),
    auth_distinct as (
        select distinct
            RiskTreatmentPlanProvision_TenantId, RiskTreatmentPlanProvision_RiskTreatmentPlanId, Authority_Name
        from auth_prov
    ),
    prov as (
        select
            RiskTreatmentPlanProvision_TenantId,
            RiskTreatmentPlanProvision_RiskTreatmentPlanId,
            STRING_AGG(AuthorityProvision_Name, ', ') as ProvisionList
        from prov_distinct
        group by RiskTreatmentPlanProvision_TenantId, RiskTreatmentPlanProvision_RiskTreatmentPlanId
    ),
    auth as (
        select
            RiskTreatmentPlanProvision_TenantId,
            RiskTreatmentPlanProvision_RiskTreatmentPlanId,
            STRING_AGG(Authority_Name, ', ') as AuthorityList
        from auth_distinct
        group by RiskTreatmentPlanProvision_TenantId, RiskTreatmentPlanProvision_RiskTreatmentPlanId
    )
select
    -- Risk level 
    risk.Risk_Id,
    risk.Risk_TenantId,
    risk.Risk_Title,
    risk.Risk_Description,
    rtag.RiskTagList,
    ctrl.ControlList,
    trt_ctrl.TreatmentPlan_Controls,
    auth.AuthorityList,
    prov.ProvisionList,
    -- Treatment and Plan Level
    trt.RiskTreatment_StatusCode,
    trt.RiskTreatment_DecisionCode,
    trt_plan.RiskTreatmentPlan_Id,
    trt_plan.RiskTreatmentPlan_StatusCode,
    trt_plan.RiskTreatmentPlan_TreatmentName,
    trt_plan.RiskTreatmentPlan_DueDateStatus,
    trt_plan.RiskTreatmentPlan_TreatmentDescription,
    trt_plan.RiskTreatmentPlan_TreatmentDate TreatmentDueDate,
    [owner].OwnerList,
    rtpc.RiskTreatmentPlanComment_Id,
    rtpc.RiskTreatmentPlanComment_CreationTime,
    format(rtpc.RiskTreatmentPlanComment_CreationTime, 'dd MMM, yyyy') Commented_Date,
    case
        when rtpc.RiskTreatmentPlanComment_UserId = au.AbpUsers_Id then au.AbpUsers_FullName else au.AbpUsers_UserName
    end Commented_Name,
    rtpc.RiskTreatmentPlanComment_Comment Treatment_Comment,
    case when row_number() over(partition by trt_plan.RiskTreatmentPlan_Id order by rtpc.RiskTreatmentPlanComment_CreationTime DESC) = 1 then 1 else 0 end TreatmentLatestComment_Flag
from risk
left join rtag on risk.Risk_Id = rtag.RiskTag_RiskId
left join trt on risk.Risk_Id = trt.Risk_Id
left join trt_plan on risk.Risk_Id = trt_plan.RiskTreatment_RiskId
left join
    ctrl
    on trt_plan.RiskTreatmentPlan_Id = ctrl.RiskTreatmentPlanControl_RiskTreatmentPlanId
    and trt_plan.RiskTreatmentPlan_TenantId = ctrl.RiskTreatmentPlanControl_TenantId
left join
    trt_ctrl
    on trt_plan.RiskTreatmentPlan_Id = trt_ctrl.RiskTreatmentPlanControl_RiskTreatmentPlanId
    and trt_plan.RiskTreatmentPlan_TenantId = trt_ctrl.RiskTreatmentPlanControl_TenantId
left join
    auth
    on trt_plan.RiskTreatmentPlan_Id = auth.RiskTreatmentPlanProvision_RiskTreatmentPlanId
    and trt_plan.RiskTreatmentPlan_TenantId = auth.RiskTreatmentPlanProvision_TenantId
left join
    prov
    on trt_plan.RiskTreatmentPlan_Id = prov.RiskTreatmentPlanProvision_RiskTreatmentPlanId
    and trt_plan.RiskTreatmentPlan_TenantId = prov.RiskTreatmentPlanProvision_TenantId
left join
    [owner]
    on trt_plan.RiskTreatmentPlan_Id = owner.RiskTreatmentPlanOwner_RiskTreatmentPlanId
    and trt_plan.RiskTreatmentPlan_TenantId = owner.RiskTreatmentPlanOwner_TenantId
left join
    {{ ref("vwRiskTreatmentPlanComment") }} rtpc
    on rtpc.RiskTreatmentPlanComment_RiskTreatmentPlanId = trt_plan.RiskTreatmentPlan_Id
left join
    {{ ref("vwAbpUser") }} au
    on au.AbpUsers_Id = rtpc.RiskTreatmentPlanComment_UserId
    and au.AbpUsers_TenantId = rtpc.RiskTreatmentPlanComment_TenantId
