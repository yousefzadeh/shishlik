{{ config(materialized="view") }}

/*
 * Multi valued attribute list of Risk
 * Risks +
 * Tags +
 * Assessments +
 * Custom Attributes
 * Risk Assessment Tags 
 * Assoc Controls +
 * Associated Provisions +
 * Associated Assets +
 * Associated Assessments name, description +
 * Associated questions
 * Associated responses
 * Associated vendors (Thirdparty) +

 * UserList - RiskUser
 * TeamRiskRating aggregated rating - TeamRiskRating
 * RiskActivity
 */
with
    grain as (
        -- one row per risk assessment or assessment if risk assessment does not exist
        select risk.Risk_Id as RiskId, risk.Risk_Name, risk.Risk_Description, risk.Risk_TenantId, wfs.WorkflowStage_Name 
        from {{ ref("vwRisk") }} risk
        left join {{ ref("vwWorkflowStage") }} wfs on wfs.WorkflowStage_Id = risk.Risk_WorkflowStageId
    ),
    -- RiskId grain
    rtag as (
        -- RiskTag_Tags
        -- one row for each RiskId
        select rt.RiskTag_RiskId, left(STRING_AGG(cast(t.Tags_Name as nvarchar(max)), ', '), 4000) as RiskTagList, RiskTag_TenantId Tenant_Id
        from {{ ref("vwRiskTag") }} rt
        inner join {{ ref("vwTags") }} t on rt.RiskTag_TagId = t.Tags_ID and rt.RiskTag_TenantId = t.Tags_TenantId
        group by rt.RiskTag_RiskId, RiskTag_TenantId
    ),
        trt_plan as (
        -- RiskTag_Tags
        -- one row for each RiskId
        select rtpa.RiskTreatmentPlanAssociation_TenantId, rtpa.RiskTreatmentPlanAssociation_RiskId, STRING_AGG(cast(rtp.RiskTreatmentPlan_TreatmentName as nvarchar(max)), ', ')as RiskTreatmentPlanList
        from {{ ref("vwRiskTreatmentPlanAssociation") }} rtpa
        inner join {{ ref("vwRiskTreatmentPlan") }} rtp on rtpa.RiskTreatmentPlanAssociation_RiskTreatmentPlanId = rtp.RiskTreatmentPlan_Id
        group by rtpa.RiskTreatmentPlanAssociation_TenantId, rtpa.RiskTreatmentPlanAssociation_RiskId
    ),
    ctrl as (
        -- RiskControl_Controls
        -- one row per riskId
        select
            rc.RiskControl_RiskId, left(STRING_AGG(cast(c.Controls_Name as nvarchar(max)), ', '), 4000) as ControlList, rc.RiskControl_TenantId Tenant_Id 
        from {{ ref("vwRiskControl") }} rc
        inner join {{ ref("vwControls") }} c on rc.RiskControl_ControlId = c.Controls_Id and rc.RiskControl_TenantId = c.Controls_TenantId
        join {{ ref("vwPolicyDomain") }} pd on pd.PolicyDomain_Id = c.Controls_PolicyDomainId and pd.PolicyDomain_TenantId = c.Controls_TenantId
        left join {{ ref("vwPolicy") }} p on p.Policy_Id = pd.PolicyDomain_PolicyId and p.Policy_Status != 100 and p.Policy_TenantId = c.Controls_TenantId
        group by rc.RiskControl_RiskId, rc.RiskControl_TenantId
    ),
    ctrl_detailed as (
        -- RiskControl_Controls
        -- one row per riskId
        select
            rc.RiskControl_RiskId,
            left(
                STRING_AGG(c.Controls_Reference + ' ' + cast(c.Controls_Name as nvarchar(max)), '; '), 4000
            ) as Findex_ControlList, 
            rc.RiskControl_TenantId Tenant_Id
        from {{ ref("vwRiskControl") }} rc
        inner join {{ ref("vwControls") }} c on rc.RiskControl_ControlId = c.Controls_Id and rc.RiskControl_TenantId = c.Controls_TenantId
        join {{ ref("vwPolicyDomain") }} pd on pd.PolicyDomain_Id = c.Controls_PolicyDomainId and pd.PolicyDomain_TenantId = c.Controls_TenantId
        join {{ ref("vwPolicy") }} p on p.Policy_Id = pd.PolicyDomain_PolicyId and p.Policy_Status != 100 and p.Policy_TenantId = c.Controls_TenantId
        group by rc.RiskControl_RiskId, rc.RiskControl_TenantId 
    ),
    auth_prov as (
        select rp.RiskProvision_RiskId, a.Authority_Name, ap.AuthorityProvision_Name, rp.RiskProvision_TenantId Tenant_Id
        from {{ ref("vwRiskProvision") }} rp
        inner join
            {{ ref("vwDirectAuthorityProvision") }} ap on rp.RiskProvision_AuthorityProvisionId = ap.AuthorityProvision_Id and rp.RiskProvision_TenantId = ap.Tenant_Id
        inner join {{ ref("vwDirectAuthority") }} a on ap.Authority_Id = a.Authority_Id and ap.Tenant_Id = a.Tenant_Id
    ),
    prov_distinct as (select distinct RiskProvision_RiskId, AuthorityProvision_Name from auth_prov),
    auth_distinct as (select distinct RiskProvision_RiskId, Authority_Name from auth_prov),
    prov as (
        select
            RiskProvision_RiskId,
            left(STRING_AGG(cast(AuthorityProvision_Name as nvarchar(max)), ', '), 4000) as AuthorityProvisionList
        from prov_distinct
        group by RiskProvision_RiskId
    ),
    auth as (
        select
            RiskProvision_RiskId, left(STRING_AGG(cast(Authority_Name as nvarchar(max)), ', '), 4000) as AuthorityList
        from auth_distinct
        group by RiskProvision_RiskId
    ),
    asset as (
        -- RiskAsset_Asset
        -- One row per riskId
        select IssueRisk_RiskId, left(STRING_AGG(cast(Asset_Title as nvarchar(max)), ', '), 4000) as AssetList, Tenant_Id
        from
            (
                select distinct ra.IssueRisk_RiskId, a.Asset_Title, ra.IssueRisk_TenantId Tenant_Id
                from {{ ref("vwIssueRisk") }} ra
                inner join {{ ref("vwAsset") }} a on ra.IssueRisk_IssueId = a.Asset_Id and ra.IssueRisk_TenantId = a.Asset_TenantId
            ) as T
        group by IssueRisk_RiskId, Tenant_Id
    ),
    rrr as (
        select rrr.RiskRegisterRecord_RiskId, r.Register_RegisterName, rr.RegisterRecord_Name, rrr.RiskRegisterRecord_TenantId Tenant_Id
        from {{ ref("vwRiskRegisterRecord") }} rrr
        join {{ ref("vwRegisterRecord") }} rr on rrr.RiskRegisterRecord_RegisterRecordId = rr.RegisterRecord_Id and rrr.RiskRegisterRecord_TenantId = rr.RegisterRecord_TenantId
        join {{ ref("vwRegister") }} r on rr.RegisterRecord_RegisterId = r.Register_Id and rr.RegisterRecord_TenantId = r.Register_TenantId
        group by rrr.RiskRegisterRecord_RiskId, r.Register_RegisterName, rr.RegisterRecord_Name, rrr.RiskRegisterRecord_TenantId
    ),
    register as (
        select T.Register_RiskId, string_agg(Register_RecordList, ', ') as RegisterList, T.Tenant_Id
        from
            (
                select
                    rrr.RiskRegisterRecord_RiskId Register_RiskId,
                    rrr.Register_RegisterName Register_Name,
                    left(string_agg(cast(rrr.RegisterRecord_Name as nvarchar(max)), ', '), 4000) as Register_RecordList,
                    rrr.Tenant_Id
                from rrr
                group by rrr.RiskRegisterRecord_RiskId, rrr.Register_RegisterName, rrr.Tenant_Id
            ) as T
        group by T.Register_RiskId, T.Tenant_Id
    ),
    ass_distinct as (
        select distinct AssessmentRisk_RiskId, Assessment_Name, Assessment_TenantId Tenant_Id
        from {{ ref("vwAssessmentRisk") }} ar
        join {{ ref("vwAssessment") }} a on ar.AssessmentRisk_AssessmentId = a.Assessment_ID and ar.AssessmentRisk_TenantId = a.Assessment_TenantId
        where a.Assessment_IsDeprecatedAssessmentVersion = 0
    ),
    ass as (
        -- AssessmentRisk_Assessment
        -- One row per RiskId
        select
            AssessmentRisk_RiskId, left(STRING_AGG(cast(Assessment_Name as nvarchar(max)), ', '), 4000) AssessmentList, Tenant_Id
        from ass_distinct
        group by AssessmentRisk_RiskId, Tenant_Id
    ),
    risk_domain as (
        select Risk_Id, left(STRING_AGG(cast(CustomField_Value as nvarchar(max)), ', '), 4000) DomainList, Tenant_Id
        from {{ ref("vwRiskCustomFieldValue") }} domain
        where CustomField_InternalDefaultName = 'RiskDomain'
        group by Risk_Id, Tenant_Id
    ),
    non_deprec_questions as (
        select
            Question_Id,
            case
                when len(Question_IdRef + ' : ' + Question_Name) > 80
                then
                    left(Question_IdRef + ' : ' + Question_Name, 43)
                    + '...'
                    + right(Question_IdRef + ' : ' + Question_Name, 37)
                else Question_IdRef + ' : ' + Question_Name
            end Question_IdRefName,
            Question_TenantId
        from {{ ref("vwAssessment") }} a
        join {{ ref("vwAssessmentDomain") }} ad on a.Assessment_Id = ad.AssessmentDomain_AssessmentId and a.Assessment_TenantId = ad.AssessmentDomain_TenantId
        join
            {{ ref("vwQuestion") }} q
            on ad.AssessmentDomain_ID = q.Question_AssessmentDomainId and ad.AssessmentDomain_TenantId = q.Question_TenantId
        where a.Assessment_IsDeprecatedAssessmentVersion = 0
    ),
    question as (
        -- AssessmentRisk_Question
        -- One row per RiskId
        select
            ar.AssessmentRisk_RiskId,
            left(
                STRING_AGG(cast(q.Question_IdRefName as nvarchar(max)), ', ') within group (
                    order by q.Question_IdRefName
                ),
                4000
            ) QuestionList,
            ar.AssessmentRisk_TenantId Tenant_Id
        from {{ ref("vwAssessmentRisk") }} ar
        join non_deprec_questions q on ar.AssessmentRisk_QuestionId = q.Question_ID and ar.AssessmentRisk_TenantId = q.Question_TenantId
        -- where q.Question_Id in ( 
        -- )
        group by ar.AssessmentRisk_RiskId, ar.AssessmentRisk_TenantId
    ),
    response as (
        -- AssessMentRisk_AsessmentResponse
        -- One row per RiskId
        select
            T.AssessmentRisk_RiskId,
            left(
                STRING_AGG(cast(T.Answer_AnswerText as nvarchar(max)), ', ') within group (
                    order by T.Answer_AnswerText
                ),
                4000
            ) as ResponseList,
            T.AssessmentRisk_TenantId Tenant_Id
        from
            (
                select
                    ar.AssessmentRisk_RiskId,
                    ar.AssessmentRisk_Id,
                    ar.AssessmentRisk_AssessmentId,
                    ar.AssessmentRisk_AssessmentDomainId,
                    ar.AssessmentRisk_QuestionId,
                    a.Answer_AnswerText,
                    ar.AssessmentRisk_TenantId
                from {{ ref("vwAssessmentRisk") }} ar
                join non_deprec_questions q on ar.AssessmentRisk_QuestionId = q.Question_Id and ar.AssessmentRisk_TenantId = q.Question_TenantId
                join {{ ref("vwAnswerSelected") }} a on q.Question_Id = a.Answer_QuestionId and q.Question_TenantId = a.Answer_TenantId
            ) as T
        group by T.AssessmentRisk_RiskId, T.AssessmentRisk_TenantId
    ),
    party as (
        -- RiskThirdParty_TenantVendor
        -- One row per RiskId
        select
            rtp.RiskThirdParty_RiskId,
            left(STRING_AGG(cast(tv.TenantVendor_Name as nvarchar(max)), ', '), 4000) VendorList,
            rtp.RiskThirdParty_TenantId Tenant_Id
        from {{ ref("vwRiskThirdParty") }} rtp
        join {{ ref("vwTenantVendor") }} tv on rtp.RiskThirdParty_TenantVendorId = tv.TenantVendor_Id and rtp.RiskThirdParty_TenantId = tv.TenantVendor_TenantId
        group by rtp.RiskThirdParty_RiskId, rtp.RiskThirdParty_TenantId
    ),
    [user] as (
        select distinct
            ru.RiskUser_RiskId,
            left(
                STRING_AGG(coalesce(ru.RiskUser_FullName, ru.RiskUser_OrganisationName), ', ') within group (
                    order by coalesce(ru.RiskUser_FullName, ru.RiskUser_OrganisationName)
                ),
                4000
            ) as RiskUser_UserList,
            count(*) user_count,
            ru.RiskUser_TenantId Tenant_Id
        from {{ ref("vwRiskUser") }} ru
        group by RiskUser_RiskId, RiskUser_TenantId
    ),
    [owner] as (
        select
            ro.RiskOwner_RiskId,
            left(
                STRING_AGG(coalesce(ro.RiskOwner_FullName, ro.RiskOwner_OrganisationName), ', ') within group (
                    order by coalesce(ro.RiskOwner_FullName, ro.RiskOwner_OrganisationName)
                ),
                4000
            ) as RiskOwner_UserList,
            count(*) user_count,
            ro.RiskOwner_TenantId Tenant_Id
        from {{ ref("vwRiskOwner") }} ro
        group by ro.RiskOwner_RiskId, ro.RiskOwner_TenantId
    )
    {#- workflowstage as (
        select r.Risk_Id, wfs.WorkflowStage_Name
        from {{ ref("vwRisk") }} r
        join {{ ref("vwWorkflowStage") }} wfs on wfs.WorkflowStage_Id = r.Risk_WorkflowStageId
    ) #}
select distinct
    grain.RiskId,
    grain.Risk_Name,
    grain.Risk_TenantId,
    grain.Risk_Description,
    rtag.RiskTagList Risk_TagList,
    trt_plan.RiskTreatmentPlanList,
    ctrl.ControlList Risk_ControlList,
    ctrl_detailed.Findex_ControlList,
    auth.AuthorityList Risk_AuthorityList,
    prov.AuthorityProvisionList Risk_AuthorityProvisionList,
    asset.AssetList Risk_AssetList,
    ass.AssessmentList Risk_AssessmentList,
    risk_domain.DomainList Risk_DomainList,

    /*Testing out a new way of writing this logic to deal:
    This version does not use OUTER APPLY, so it avoids the ANSI_NULLS error cause by below CTE.
    STRING_AGG(... WITHIN GROUP ORDER BY ...) ensures ordered aggregation.*/ 

	LEFT((
			SELECT STRING_AGG(CAST(ch.CustomField_Level2Value AS varchar(400)), ', ')
				   WITHIN GROUP (ORDER BY ch.CustomField_Level2Value)
			FROM {{ ref("vwRiskChildCustomFieldValue") }} ch
			WHERE CAST(ch.Risk_Id AS BIGINT) = grain.RiskId
			  AND CAST(ch.Tenant_Id AS BIGINT) = grain.Risk_TenantId
			  AND ch.CustomField_InternalDefaultName = 'RiskDomain'
			FOR XML PATH(''), TYPE
		).value('.', 'nvarchar(max)'), 4000) AS Risk_ChildDomainList ,
    LEFT((
            SELECT STRING_AGG(CAST(gc.CustomField_Level3Value AS varchar(400)), ', ')
						WITHIN GROUP (ORDER BY gc.CustomField_Level3Value)
            FROM {{ ref("vwRiskGrandChildCustomFieldValue") }} gc
            WHERE CAST(gc.Risk_Id AS BIGINT) = grain.RiskId
              AND CAST(gc.Tenant_Id AS BIGINT) = grain.Risk_TenantId
              AND gc.CustomField_InternalDefaultName = 'RiskDomain'
            FOR XML PATH(''), TYPE
        ).value('.', 'nvarchar(max)'),4000) AS Risk_GrandchildDomainList,

    question.QuestionList Risk_QuestionList,
    response.ResponseList Risk_ResponseList,
    party.VendorList Risk_VendorList,
    [user].RiskUser_UserList Risk_UserList,
    [owner].RiskOwner_UserList Risk_OwnerList,
    [register].RegisterList Risk_RegisterList,
    grain.WorkflowStage_Name
from grain
left join rtag on grain.RiskId = rtag.RiskTag_RiskId and grain.Risk_TenantId = rtag.Tenant_Id 
left join trt_plan on grain.RiskId = trt_plan.RiskTreatmentPlanAssociation_RiskId and grain.Risk_TenantId = trt_plan.RiskTreatmentPlanAssociation_TenantId 
left join ctrl on grain.RiskId = ctrl.RiskControl_RiskId and grain.Risk_TenantId = ctrl.Tenant_Id
left join ctrl_detailed on grain.RiskId = ctrl_detailed.RiskControl_RiskId and grain.Risk_TenantId = ctrl_detailed.Tenant_Id
left join prov on grain.RiskId = prov.RiskProvision_RiskId 
left join auth on grain.RiskId = auth.RiskProvision_RiskId 
left join asset on grain.RiskId = asset.IssueRisk_RiskId and grain.Risk_TenantId = asset.Tenant_Id
left join ass on grain.RiskId = ass.AssessmentRisk_RiskId and grain.Risk_TenantId = ass.Tenant_Id
left join risk_domain on grain.RiskId = risk_domain.Risk_Id and grain.Risk_TenantId = risk_domain.Tenant_Id
left join question on grain.RiskId = question.AssessmentRisk_RiskId and grain.Risk_TenantId = question.Tenant_Id
left join response on grain.RiskId = response.AssessmentRisk_RiskId and grain.Risk_TenantId = response.Tenant_Id
left join party on grain.RiskId = party.RiskThirdParty_RiskId and grain.Risk_TenantId = party.Tenant_Id
left join [user] on grain.RiskId = [user].RiskUser_RiskId and grain.Risk_TenantId = [user].Tenant_Id
left join [owner] on grain.RiskId = [owner].RiskOwner_RiskId and grain.Risk_TenantId = [owner].Tenant_Id
left join [register] on grain.RiskId = [register].Register_RiskId and grain.Risk_TenantId = [register].Tenant_Id

-- OUTER APPLY (
--     SELECT LEFT(
--         (
--             SELECT STRING_AGG(CAST(ch.CustomField_Level2Value AS varchar(400)), ', ')
--             FROM {{ ref("vwRiskChildCustomFieldValue") }} ch
--             WHERE CAST(ch.Risk_Id AS BIGINT) = grain.RiskId
--               AND CAST(ch.Tenant_Id AS BIGINT) = grain.Risk_TenantId
--               AND ch.CustomField_InternalDefaultName = 'RiskDomain'
--             FOR XML PATH(''), TYPE
--         ).value('.', 'nvarchar(max)'),
--     4000) AS DomainList
-- ) cd

-- OUTER APPLY (
--     SELECT LEFT(
--         (
--             SELECT STRING_AGG(CAST(gc.CustomField_Level3Value AS varchar(400)), ', ')
--             FROM {{ ref("vwRiskGrandChildCustomFieldValue") }} gc
--             WHERE CAST(gc.Risk_Id AS BIGINT) = grain.RiskId
--               AND CAST(gc.Tenant_Id AS BIGINT) = grain.Risk_TenantId
--               AND gc.CustomField_InternalDefaultName = 'RiskDomain'
--             FOR XML PATH(''), TYPE
--         ).value('.', 'nvarchar(max)'),
--     4000) AS DomainList
-- ) gd