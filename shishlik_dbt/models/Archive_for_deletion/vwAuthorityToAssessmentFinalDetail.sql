{{ config(materialized="view") }}

{# 

Replicating the Authority to Assessment Mapping Report
https://dev-ihsopk.6clicks.io/app/main/reporting/authority-assessment-mapping-report

Documentation:
https://dev.azure.com/admin0011/6clicks/_wiki/wikis/6clicks.wiki/278/Reports

Security:
Client filtering is on TenantId in Authority_TenantId columnn

Grain:
1. Authority
2.1 Assessment Template
2.2 Assessment Template Version
2.3 Assessment
3. Provision

Filters:
(AuthorityId) Authority Name - this view
Assessment Template Name - this view
Assessment Template Version - this view
Vendor - join
(AssessmentId) Assessment Name - this view
RollUp method (Fixed options 'Min', 'Max', Average)
Actual Result

Presentation:
Style: Crosstab
Crosstab Columns: #3 Assessments
Crosstab Rows:    #4 Provision
Metric: 
Base metric is "Answer_Score"

Roll ups are made in vwAuthorityToAssessmentReport to match the output in 6Clicks report.
Read there for logic of rollups to replicate in Yellowfin.

If Template_AssessmentStyle is "Risk Rated" then use "score labels"
If Template_AssessmentStyle is "Weighted Score" then use "score"
The metric is aggregated 3 ways depending on the filter value of the Rollup Method (min, max, average)

Provision Custom fields:
In the existing report, only the first column (CustomFieldOrder=1) is shown as a column with the column name as CustomFieldName and value as CustomValue
In YF, all custom fields may be shown as a popup drill through.

How to use the views in Yellowfin:
1. vwAuthorityToAssessmentDetail is the main view for fact table
2. Join vwAuthorityProvisionCustomValue contains the Custom Fields for drillthru
3. Join vwTenantVendor contains the Vendor Name in TenantVendor_Name for filter

 #}
with
    tpl as (
        {# Assessment Template #}
        select
            case when Template_AuthorityId is null then 'Thru Control Set' else 'Direct' end Relation,
            coalesce(Template_AuthorityId, ap.AuthorityPolicy_AuthorityId) as Template_RelatedAuthorityId,
            t.*
        from {{ ref("vwAssessmentTemplate") }} t
        left join {{ ref("vwAuthorityPolicy") }} ap on t.Template_PolicyId = ap.AuthorityPolicy_PolicyId
        where t.Template_IsTemplate = 1
    ),
    ass as (
        {# Only QBA Assessments are considered #}
        select *
        from {{ ref("vwAssessment") }} a
        where
            a.Assessment_WorkFlow = 'Question'  -- QBA only
            and a.Assessment_IsTemplate = 0  -- assessments only
            and a.Assessment_IsArchived = 0
            and a.Assessment_IsDeprecatedAssessmentVersion = 0
            and a.Assessment_Status in (4, 5, 6)  -- completed
    ),
    tenant_auth as (
        {# Need distinct after union as it returns mutiple rows #}
        select distinct *
        from
            (
                select TenantAuthority_AuthorityId Authority_Id, a.Authority_Name, ta.TenantAuthority_TenantId Tenant_Id
                from {{ ref("vwTenantAuthority") }} ta
                join {{ ref("vwAuthority") }} a on a.Authority_Id = ta.TenantAuthority_AuthorityId

                union all

                select Authority_Id, Authority_Name, Authority_TenantId
                from {{ ref("vwAuthority") }} a
            ) as T
    ),
    auth_to_ass as (
        {# 
  Grain of report:
  Tenant ID
  Authority ID
  Assessment Template ID
  Assessment ID
  Provision ID
 #}
        select
            tenant_auth.Tenant_Id,
            tenant_auth.Authority_Id,
            tenant_auth.Authority_Name,
            tpl.Template_Id,
            tpl.Template_Name,
            tpl.Template_TemplateVersion,
            tpl.Template_QuestionTypeCode Template_AssessmentStyle,
            ass.Assessment_Id,
            ass.Assessment_Name,
            ass.Assessment_TenantVendorId,
            ass.Assessment_EngagementId
        from tenant_auth
        join tpl on tenant_auth.Authority_Id = tpl.Template_RelatedAuthorityId
        join ass on ass.Assessment_CreatedFromTemplateId = tpl.Template_Id

    ),
    q_ap_direct as (
        select
            'Direct' Relation,
            pq.ProvisionQuestion_AuthorityProvisionId AuthorityProvision_Id,
            ap.AuthorityProvision_Name,
            ap.AuthorityProvision_ReferenceId,
            q.*
        from {{ ref("vwQuestionAnswerMulti") }} q
        join {{ ref("vwProvisionQuestion") }} pq on q.Answer_QuestionId = pq.ProvisionQuestion_QuestionId
        join
            {{ ref("vwAuthorityProvision") }} ap on pq.ProvisionQuestion_AuthorityProvisionId = ap.AuthorityProvision_Id
    ),
    q_ap_indirect as (
        select
            'Thru Control Set' Relation,
            pc.ProvisionControl_AuthorityReferenceId AuthorityProvision_Id,
            ap.AuthorityProvision_Name,
            ap.AuthorityProvision_ReferenceId,
            q.*
        from {{ ref("vwQuestionAnswerMulti") }} q
        join {{ ref("vwControlQuestion") }} cq on cq.ControlQuestion_QuestionId = q.Answer_QuestionId
        join {{ ref("vwProvisionControl") }} pc on cq.ControlQuestion_ControlsId = pc.ProvisionControl_ControlsId
        join {{ ref("vwAuthorityProvision") }} ap on pc.ProvisionControl_AuthorityReferenceId = ap.AuthorityProvision_Id
    ),
    q_ap as (
        select *
        from q_ap_direct

        union all

        select *
        from q_ap_indirect
    ),
    score as (
        {# 
  Grain of Data for aggregate scoring:
  Tenant ID
  Authority ID
  Assessment Template ID
  Assessment ID
  Provision ID
  Question ID

#}
        select
            q_ap.Question_AssessmentDomainId,
            q_ap.AuthorityProvision_Name,
            q_ap.AuthorityProvision_Id,
            q_ap.AuthorityProvision_ReferenceId,
            q_ap.Answer_QuestionId,
            q_ap.Question_TypeCode,
            q_ap.Question_Weighting,
            q_ap.Question_Multiplier,
            q_ap.Answer_IsCompleted,
            q_ap.Answer_RiskStatusCalc,
            q_ap.Answer_WeightedScore
        from q_ap
    )

{# 
    Replicated Report detail 
 #}
select distinct
    a.Tenant_Id,
    a.Authority_Id,
    a.Authority_Name,
    a.Template_Id,
    a.Template_Name,
    a.Template_TemplateVersion Template_Version,
    a.Template_AssessmentStyle,
    a.Assessment_Id,
    a.Assessment_Name,
    a.Assessment_TenantVendorId,
    a.Assessment_EngagementId,
    s.AuthorityProvision_Id,
    s.AuthorityProvision_ReferenceId,
    s.AuthorityProvision_Name,
    s.Answer_QuestionId,
    s.Question_TypeCode,
    s.Question_Weighting,
    {#  
--  RiskStatus is the RiskStatus stored in DB and ComponentStr
--  RiskStatusCalc is the conversion to an ordered range 0,1,2,3,4,5 for rolling up the Risk Status measures
--  WeightedScore is the Weighting column in Question table X Selected Answer Multiplier in 'rank' attribute of Question ComponentStr 
 #}
    case
        when a.Template_AssessmentStyle = 'Risk Rated'
        then s.Answer_RiskStatusCalc
        when a.Template_AssessmentStyle = 'Weighted Score'
        then s.Answer_WeightedScore
        else 0
    end Answer_Score,
    s.Answer_IsCompleted
from auth_to_ass as a
join {{ ref("vwAssessmentDomain") }} ad on ad.AssessmentDomain_AssessmentId = a.Assessment_Id
left join score as s on ad.AssessmentDomain_Id = s.Question_AssessmentDomainId
