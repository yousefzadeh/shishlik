{{ config(materialized="view") }}

/*
 * Multi valued attribute list of issues
 * Issues -> assessments, tags, texts, risks
 */
with
    iss as (select Issues_Id, Issues_TenantId, Issues_Stage, Issues_StageCode from {{ ref("vwIssues") }} i where i.Issues_Status != 100),
    ass_name_distinct as (
        select distinct
            ia.IssueAssessment_IssueId,
            ia.IssueAssessment_TenantId,
            left(convert(varchar(max), a.Assessment_Name), 50) Assessment_Name
        from {{ ref("vwIssueAssessmentLink") }} ia
        inner join
            {{ ref("vwAssessment") }} a
            on ia.IssueAssessment_AssessmentId = a.Assessment_ID
            and ia.IssueAssessment_TenantId = a.Assessment_TenantId
            and a.Assessment_IsDeprecatedAssessmentVersion = 0
    ),
    ass as (
        select
            ia.IssueAssessment_IssueId,
            ia.IssueAssessment_TenantId,
            left(
                STRING_AGG(cast(ia.Assessment_Name as nvarchar(max)), ', ') within group (order by ia.Assessment_Name),
                4000
            ) as AssessmentList
        from ass_name_distinct ia
        group by ia.IssueAssessment_IssueId, ia.IssueAssessment_TenantId
    ),
    ass_link as (
        select ia.*
        from {{ ref("vwIssueAssessmentLink") }} ia
        inner join
            {{ ref("vwAssessment") }} a
            on ia.IssueAssessment_AssessmentId = a.Assessment_ID
            and ia.IssueAssessment_TenantId = a.Assessment_TenantId
            and a.Assessment_IsDeprecatedAssessmentVersion = 0
    ),
    ass_q_table as (
        select distinct
            Question_Id,
            Question_AssessmentDomainId,
            case
                when len(Question_IdRef + ' : ' + Question_Name) > 80
                then
                    left(Question_IdRef + ' : ' + Question_Name, 43)
                    + '...'
                    + right(Question_IdRef + ' : ' + Question_Name, 37)
                else Question_IdRef + ' : ' + Question_Name
            end Question_IdRefName
        from {{ ref("vwQuestion") }} q
        join ass_link ia on ia.IssueAssessment_QuestionId = q.Question_Id
    ),
    ass_q as (
        select
            ia.IssueAssessment_IssueId,
            ia.IssueAssessment_TenantId,
            left(
                STRING_AGG(convert(varchar(max), q.Question_IdRefName), ', ') within group (
                    order by q.Question_IdRefName
                ),
                4000
            ) as QuestionList
        from ass_link ia
        inner join ass_q_table q on ia.IssueAssessment_QuestionId = q.Question_Id
        group by ia.IssueAssessment_IssueId, ia.IssueAssessment_TenantId
    ),
    ass_resp as (
        select
            ia.IssueAssessment_IssueId,
            ia.IssueAssessment_TenantId,
            left(
                STRING_AGG(convert(varchar(max), ans.Answer_AnswerText), ', ') within group (
                    order by ans.Answer_AnswerText
                ),
                4000
            ) as ResponseList
        from ass_link ia
        inner join {{ ref("vwAnswerSelected") }} ans on ans.Answer_QuestionId = ia.IssueAssessment_QuestionId
        group by ia.IssueAssessment_IssueId, ia.IssueAssessment_TenantId
    ),
    tag as (
        select
            it.IssueTag_IssueId,
            t.Tags_TenantId,
            left(
                STRING_AGG(left(convert(varchar(max), t.Tags_Name), 50), ', ') within group (order by t.Tags_Name), 4000
            ) as TagList
        from {{ ref("vwIssueTag") }} it
        inner join {{ ref("vwTags") }} t on it.IssueTag_TagId = t.Tags_ID and it.IssueTag_TenantId = t.Tags_TenantId
        group by it.IssueTag_IssueId, Tags_TenantId
    ),
    txt1 as (
        select distinct icdft.IssueFreeTextControlData_IssueId, icdft.CustomLabel + '=' + left(icdft.Value, 50) TextData
        from {{ ref("vwIssueCustomDataFreeText") }} icdft
    ),
    txt as (
        select
            txt1.IssueFreeTextControlData_IssueId,
            left(
                STRING_AGG(convert(varchar(max), txt1.TextData), ', ') within group (order by txt1.TextData), 4000
            ) as TextDataList
        from txt1
        group by txt1.IssueFreeTextControlData_IssueId
    ),
    risk as (
        select
            IssueRisk_IssueId,
            IssueRisk_TenantId,
            left(STRING_AGG(left(convert(varchar(max), r.Risk_Name), 50), ', '), 1000) as RiskList
        from {{ ref("vwIssueRisk") }} ir
        inner join {{ ref("vwRisk") }} r on ir.IssueRisk_RiskId = r.Risk_Id and ir.IssueRisk_TenantId = r.Risk_TenantId
        group by IssueRisk_IssueId, IssueRisk_TenantId
    ),
    owner1 as (
        select distinct
            IssueOwner_TenantId,
            IssueOwner_IssueId,
            IssueOwner_FullName as OwnerText,
            IssueOwner_OrganisationName as OrganizationText
        from {{ ref("vwIssueOwner") }}
    ),
    [owner] as (
        select
            IssueOwner_TenantId,
            IssueOwner_IssueId,
            left(
                STRING_AGG(convert(varchar(max), coalesce(OwnerText, OrganizationText)), ', ') within group (
                    order by COALESCE(OwnerText, OrganizationText)
                ),
                4000
            ) as OwnerList
        from owner1
        group by IssueOwner_TenantId, IssueOwner_IssueId
    ),
    user1 as (
        select IssueUser_TenantId, IssueUser_IssueId, IssueUser_FullName as UserText from {{ ref("vwIssueUser") }} iu
    ),
    [user] as (
        select
            IssueUser_TenantId,
            IssueUser_IssueId,
            left(
                STRING_AGG(convert(varchar(max), coalesce(UserText, 'No User')), ', ') within group (order by UserText),
                4000
            ) as UserList
        from user1
        group by IssueUser_TenantId, IssueUser_IssueId
    ),
    thirdparty as (
        select
            tp.IssueThirdParty_IssueId,
            tp.IssueThirdParty_TenantId,
            left(STRING_AGG(convert(varchar(max), tv.TenantVendor_Name), ', '), 4000) as ThirdPartyList
        from {{ ref("vwIssueThirdParty") }} tp
        inner join {{ ref("vwTenantVendor") }} tv on tp.IssueThirdParty_TenantVendorId = tv.TenantVendor_Id
        group by tp.IssueThirdParty_IssueId, tp.IssueThirdParty_TenantId
    ),
    control_set as (
        select
            ics.IssueControlStatement_IssueId,
            ics.IssueControlStatement_TenantId,
            left(STRING_AGG(convert(varchar(max), p.Policy_Name), ', '), 4000) as ControlSetList
        from {{ ref("vwIssueControlStatement") }} ics
        inner join
            {{ ref("vwControls") }} c
            on ics.IssueControlStatement_ControlId = c.Controls_Id
            and ics.IssueControlStatement_TenantId = c.Controls_TenantId
        inner join 
            {{ ref("vwPolicyDomain") }} pd 
            on c.Controls_PolicyDomainId = pd.PolicyDomain_Id
            and c.Controls_TenantId = pd.PolicyDomain_TenantId
        inner join 
            {{ ref("vwPolicy") }} p 
            on pd.PolicyDomain_PolicyId = p.Policy_Id
            and pd.PolicyDomain_TenantId = p.Policy_TenantId
        group by ics.IssueControlStatement_IssueId, ics.IssueControlStatement_TenantId
    ),
    control as (
        select
            ics.IssueControlStatement_IssueId,
            ics.IssueControlStatement_TenantId,
            left(STRING_AGG(convert(varchar(max), c.Controls_Name), ', '), 4000) as ControlList
        from {{ ref("vwIssueControlStatement") }} ics
        inner join
            {{ ref("vwControls") }} c
            on ics.IssueControlStatement_ControlId = c.Controls_Id
            and ics.IssueControlStatement_TenantId = c.Controls_TenantId
        group by ics.IssueControlStatement_IssueId, ics.IssueControlStatement_TenantId
    ),
    control_responsibility as (
        select
            ics.IssueControlStatement_IssueId,
            ics.IssueControlStatement_TenantId,
            left(STRING_AGG(convert(varchar(max), r.Responsibility_Title), ', '), 4000) as ResponsibilityList
        from {{ ref("vwIssueControlStatement") }} ics
        inner join
            {{ ref("vwResponsibilityControl") }} rc
            on ics.IssueControlStatement_ControlId = rc.ResponsibilityControl_ControlId
            and ics.IssueControlStatement_TenantId = rc.ResponsibilityControl_TenantId
        inner join 
            {{ ref("vwResponsibility") }} r 
            on rc.ResponsibilityControl_ResponsibilityId = r.Responsibility_Id
            and rc.ResponsibilityControl_TenantId = r.Responsibility_TenantId
        group by ics.IssueControlStatement_IssueId, ics.IssueControlStatement_TenantId
    ),
    auth1 as (
        select distinct
            ip.IssueProvision_TenantId,
            ip.IssueProvision_IssueId,
            convert(nvarchar(max), a.Authority_Name) as Authority_Name
        from {{ ref("vwIssueProvision") }} ip
        inner join
            {{ ref("vwAuthorityProvision") }} ap on ip.IssueProvision_AuthorityProvisionId = ap.AuthorityProvision_Id
        inner join {{ ref("vwAuthority") }} a on ap.AuthorityProvision_AuthorityId = a.Authority_Id
    ),
    auth as (
        select
            ip.IssueProvision_TenantId,
            ip.IssueProvision_IssueId,
            STRING_AGG(cast(ip.Authority_Name as nvarchar(max)), ', ') as AuthorityList
        from auth1 as ip
        group by ip.IssueProvision_TenantId, ip.IssueProvision_IssueId
    ),
    provision1 as (
        select distinct
            ip.IssueProvision_TenantId,
            ip.IssueProvision_IssueId,
            convert(nvarchar(max), ap.AuthorityProvision_Name) as AuthorityProvision_name
        from {{ ref("vwIssueProvision") }} ip
        inner join
            {{ ref("vwAuthorityProvision") }} ap on ip.IssueProvision_AuthorityProvisionId = ap.AuthorityProvision_Id
        inner join {{ ref("vwAuthority") }} a on ap.AuthorityProvision_AuthorityId = a.Authority_Id
    ),
    provision as (
        select
            ip.IssueProvision_TenantId,
            ip.IssueProvision_IssueId,
            STRING_AGG(cast(ip.AuthorityProvision_Name as nvarchar(max)), ', ') as ProvisionList
        from provision1 ip
        group by ip.IssueProvision_TenantId, ip.IssueProvision_IssueId
    ),
    thirdpartycontrol as (
        select distinct iftcd.IssueFreeTextControlData_IssueId, tpc.ThirdPartyControl_EntityType
        from {{ ref("vwIssueFreeTextControlData") }} iftcd
        inner join
            {{ ref("vwThirdPartyControl") }} tpc
            on iftcd.IssueFreeTextControlData_ThirdPartyControlId = tpc.ThirdPartyControl_Id
    ),
    entity as (
        select distinct
            iftcd.IssueFreeTextControlData_IssueId,
            tpc.ThirdPartyControl_EntityType,
            case
                when tpc.ThirdPartyControl_EntityType = 0
                then 'Third-Party'
                when tpc.ThirdPartyControl_EntityType = 1
                then 'Asset'
                when tpc.ThirdPartyControl_EntityType = 2
                then 'Risk'
                when tpc.ThirdPartyControl_EntityType = 3
                then 'Risk Treatment'
                when tpc.ThirdPartyControl_EntityType = 4
                then 'Risk Assessment'
                when tpc.ThirdPartyControl_EntityType = 5
                then 'Register'
                when tpc.ThirdPartyControl_EntityType = 6
                then 'Issue'
                else 'Undefined'
            end as ThirdPartyControl_EntityTypeCode
        from {{ ref("vwIssueFreeTextControlData") }} iftcd
        inner join
            {{ ref("vwThirdPartyControl") }} tpc
            on iftcd.IssueFreeTextControlData_ThirdPartyControlId = tpc.ThirdPartyControl_Id
    ),
    asset as (
        select
        ia.IssueId, ia.TenantId, left(STRING_AGG(left(convert(varchar(max), a.Asset_Title), 50), ', '), 4000) as AssetList
        from {{ source("issue_models", "IssueRegisterRecord") }} ia
        inner join {{ ref("vwAsset") }} a on ia.LinkedIssueId = a.Asset_Id and ia.TenantId = a.Asset_TenantId
        where ia.IsDeleted = 0
        group by ia.IssueId, ia.TenantId
    ),
    customregister as (
        select
            irr.IssueId,
            irr.TenantId,
            left(STRING_AGG(left(convert(varchar(max), i.Name), 50), ', '), 4000) as CustomRegisterList
        from {{ source("issue_models", "IssueRegisterRecord") }} irr
        inner join {{ source("issue_models", "Issues") }} i on irr.LinkedIssueId = i.Id and irr.TenantId = i.TenantId
        where irr.IsDeleted = 0
        group by irr.IssueId, irr.TenantId
    ),
    issuetype as (
        select distinct
            icad.IssueCustomAttributeData_IssueId,
            icad.IssueCustomAttributeData_TenantId,
            left(
                STRING_AGG(left(convert(varchar(max), tpa.ThirdPartyAttributes_Name), 50), ', '), 4000
            ) as ThirdPartyAttributesList
        from {{ ref("vwIssueCustomAttributeData") }} icad
        left join
            {{ ref("vwThirdPartyAttributes") }} tpa
            on icad.IssueCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
        group by icad.IssueCustomAttributeData_IssueId, icad.IssueCustomAttributeData_TenantId

    ),
    vulnerability as (
        select 
        vi.VulnerabilityIssue_TenantId,
        vi.VulnerabilityIssue_IssueId,
        string_agg(v.Vulnerability_Title, ', ') as VulnerabilityList 
        from {{ ref("vwVulnerabilityIssue") }} vi 
        join {{ ref("vwVulnerability") }} v on vi.VulnerabilityIssue_VulnerabilityId = v.Vulnerability_Id 
        group by
        vi.VulnerabilityIssue_TenantId,
        vi.VulnerabilityIssue_IssueId
    )
select
    iss.Issues_Id,
    iss.Issues_TenantId,
    iss.Issues_Stage,
    iss.Issues_StageCode,
    ass.AssessmentList,
    ass_q.QuestionList,
    ass_resp.ResponseList,
    tag.TagList,
    txt.TextDataList,
    risk.RiskList,
    [owner].OwnerList,
    [user].UserList,
    thirdparty.ThirdPartyList,
    control.ControlList,
    auth.AuthorityList,
    provision.ProvisionList,
    entity.ThirdPartyControl_EntityType,
    entity.ThirdPartyControl_EntityTypeCode,
    asset.AssetList,
    customregister.CustomRegisterList,
    issuetype.ThirdPartyAttributesList,
    control_set.ControlSetList,
    control_responsibility.ResponsibilityList,
    vulnerability.VulnerabilityList
from iss
left join ass on iss.Issues_Id = ass.IssueAssessment_IssueId and iss.Issues_TenantId = ass.IssueAssessment_TenantId
left join
    ass_q on iss.Issues_Id = ass_q.IssueAssessment_IssueId and iss.Issues_TenantId = ass_q.IssueAssessment_TenantId
left join
    ass_resp
    on iss.Issues_Id = ass_resp.IssueAssessment_IssueId
    and iss.Issues_TenantId = ass_resp.IssueAssessment_TenantId
left join tag on iss.Issues_Id = tag.IssueTag_IssueId and iss.Issues_TenantId = tag.Tags_TenantId
left join txt on iss.Issues_Id = txt.IssueFreeTextControlData_IssueId
left join risk on iss.Issues_Id = risk.IssueRisk_IssueId and iss.Issues_TenantId = risk.IssueRisk_TenantId
left join [owner] on iss.Issues_Id = [owner].IssueOwner_IssueId and iss.Issues_TenantId = [owner].IssueOwner_TenantId
left join [user] on iss.Issues_Id = [user].IssueUser_IssueId and iss.Issues_TenantId = [user].IssueUser_TenantId
left join
    thirdparty
    on iss.Issues_Id = thirdparty.IssueThirdParty_IssueId
    and iss.Issues_TenantId = thirdparty.IssueThirdParty_TenantId
left join
    control
    on iss.Issues_Id = control.IssueControlStatement_IssueId
    and iss.Issues_TenantId = control.IssueControlStatement_TenantId
left join auth on iss.Issues_Id = auth.IssueProvision_IssueId and iss.Issues_TenantId = auth.IssueProvision_TenantId
left join
    provision
    on iss.Issues_Id = provision.IssueProvision_IssueId
    and iss.Issues_TenantId = provision.IssueProvision_TenantId
left join entity on iss.Issues_Id = entity.IssueFreeTextControlData_IssueId
-- AND iss.Issues_TenantId = thirdpartycontrol.IssueFreeTextControlData_TenantId
left join thirdpartycontrol on iss.Issues_Id = thirdpartycontrol.IssueFreeTextControlData_IssueId
-- AND iss.Issues_TenantId = thirdpartycontrol.IssueFreeTextControlData_TenantId
left join asset on iss.Issues_Id = asset.IssueId and iss.Issues_TenantId = asset.TenantId
left join customregister on iss.Issues_Id = customregister.IssueId and iss.Issues_TenantId = customregister.TenantId
left join
    issuetype
    on iss.Issues_Id = issuetype.IssueCustomAttributeData_IssueId
    and iss.Issues_TenantId = issuetype.IssueCustomAttributeData_TenantId
left join 
    vulnerability
    on iss.Issues_Id = vulnerability.VulnerabilityIssue_IssueId
    and iss.Issues_TenantId = vulnerability.VulnerabilityIssue_TenantId
left join 
    control_responsibility
    on iss.Issues_Id = control_responsibility.IssueControlStatement_IssueId
    and iss.Issues_TenantId = control_responsibility.IssueControlStatement_TenantId
left join 
    control_set 
    on iss.Issues_Id = control_set.IssueControlStatement_IssueId
    and iss.Issues_TenantId = control_set.IssueControlStatement_TenantId