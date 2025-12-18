{{ config(materialized="view") }}


/****************

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

****/
with
    tpl as (
        -- Assessment Template
        select
            case
                when Template_AuthorityId is null then 'Thru Control Set' else 'Thru Authority'
            end Template_AuthorityRelation,
            coalesce(Template_AuthorityId, ap.AuthorityPolicy_AuthorityId) as Template_RelatedAuthorityId,
            t.*
        from {{ ref("vwAssessmentTemplate") }} t
        left join {{ ref("vwAuthorityPolicy") }} ap on t.Template_PolicyId = ap.AuthorityPolicy_PolicyId
        where t.Template_IsTemplate = 1 and t.Template_IsArchived = 0 and t.Template_Status in (3, 100)

    ),
    ass as (
        -- Assessment 
        select *
        from {{ ref("vwAssessment") }} a
        where
            a.Assessment_WorkFlow = 'Question'  -- QBA only
            and a.Assessment_IsTemplate = 0  -- assessments only
            and a.Assessment_IsArchived = 0
            and a.Assessment_IsDeprecatedAssessmentVersion = 0
            and a.Assessment_Status in (4, 5, 6)  -- completed
    ),
    /*******************
, authority_all as (
	select 
	TenantAuthority_AuthorityId Authority_Id, 
	a.Authority_Name,
	ta.TenantAuthority_TenantId Tenant_Id,
	'Downloaded From Tenant #' + cast(a.Authority_TenantId as varchar) Authority_LinkType 
	from {{ ref("vwTenantAuthority") }} ta
	join {{ ref("vwAuthority") }} a
		on a.Authority_Id = ta.TenantAuthority_AuthorityId

	union ALL

	select 
	Authority_Id, 
	Authority_Name,
	Authority_TenantId,
	'Direct' Authority_LinkType
	from {{ ref("vwAuthority") }} a
)
, tenant_auth as (
	 select 
	    Tenant_Id,
	    Authority_Id, 
	    Authority_Name,
		string_agg(Authority_LinkType,',') Authority_LinkType,
		count(*) row_count
	  from (
	    select distinct 
	    Authority_Id, 
	    Authority_Name,
	    Tenant_Id,
		Authority_LinkType
		from authority_all
	  ) as T
	  group by 
  	    Authority_Id, 
	    Authority_Name,
	    Tenant_Id
)
, auth_to_ass as (
 
--  Grain of report:
--  Tenant ID
--  Authority ID
--  Assessment Template ID
--  Assessment ID
--  Provision ID


	SELECT
	a.Tenant_Id Authority_TenantId, 
	tpl.Template_TenantId,
	a.Authority_Id,
	a.Authority_Name,
	a.Authority_LinkType,
	tpl.Template_RelatedAuthorityId,
	tpl.Template_AuthorityRelation,
	tpl.Template_Id,
	tpl.Template_Name,
	tpl.Template_TemplateVersion Template_Version,
	tpl.Template_QuestionTypeCode Template_AssessmentStyle, 
	ass.Assessment_Id,
	ass.Assessment_Name,
	ass.Assessment_TenantVendorId,
	ass.Assessment_EngagementId,
	ap.AuthorityProvision_Id,
	ap.AuthorityProvision_ReferenceId,
	ap.AuthorityProvision_Name
	from {{ref("vwAuthorityProvision") }} ap
	join tenant_auth a
		on ap.AuthorityProvision_AuthorityId = a.Authority_Id
	left join tpl
		on tpl.Template_RelatedAuthorityId = a.Authority_Id
	left join ass
		on ass.Assessment_CreatedFromTemplateId = tpl.Template_Id
)
********************/
    assessment_score as (

        -- Grain of Data for aggregate scoring:
        -- Tenant ID
        -- Assessment Template ID
        -- Assessment ID
        -- Provision ID
        -- Question ID
        -- Answer ID
        select
            tpl.Template_TenantId,
            tpl.Template_AuthorityId,  -- Authority ID
            tpl.Template_RelatedAuthorityId,
            tpl.Template_AuthorityRelation,
            tpl.Template_Id,
            tpl.Template_Name,
            tpl.Template_TemplateVersion Template_Version,
            tpl.Template_QuestionTypeCode Template_AssessmentStyle,
            ass.Assessment_TenantId,
            ass.Assessment_CreatedFromTemplateId,
            ass.Assessment_Id,
            ass.Assessment_Name,
            ass.Assessment_QuestionType,
            ass.Assessment_StatusCode,
            ass.Assessment_QuestionTypeCode,
            ass.Assessment_EngagementId,
            ass.Assessment_TenantVendorId,
            pq.ProvisionQuestion_AuthorityProvisionId,  -- ProvisionID
            qa.Answer_QuestionId,
            qa.Answer_Id,
            qa.Question_TypeCode,
            qa.Question_Weighting,
            qa.Question_Multiplier,
            qa.Answer_RiskStatusCode,
            qa.Answer_AnswerText,
            qa.Answer_IsCompleted,
            case
                when ass.Assessment_QuestionTypeCode = 'Risk Rated' then qa.Answer_RiskStatusCalc * 1.0
            end Answer_RiskStatusCalc,
            case
                when ass.Assessment_QuestionTypeCode = 'Weighted Score' then qa.Answer_WeightedScore * 1.0
            end Answer_WeightedScore,
            /******************** 
		RiskStatus is the RiskStatus stored in DB and ComponentStr
		RiskStatusCalc is the conversion to an ordered range 0,1,2,3,4,5 for rolling up the Risk Status measures
		WeightedScore is the Weighting column in Question table X Selected Answer Multiplier in 'rank' attribute of Question ComponentStr 
	*********************/
            case
                when ass.Assessment_QuestionTypeCode = 'Risk Rated'
                then qa.Answer_RiskStatusCalc
                when ass.Assessment_QuestionTypeCode = 'Weighted Score'
                then qa.Answer_WeightedScore
                else 0
            end Answer_Score
        from tpl
        join ass on ass.Assessment_CreatedFromTemplateId = tpl.Template_Id
        join {{ ref("vwAssessmentDomain") }} ad on ad.AssessmentDomain_AssessmentId = ass.Assessment_Id
        join {{ ref("vwQuestionAnswerMulti") }} qa on qa.Question_AssessmentDomainId = ad.AssessmentDomain_ID
        join {{ ref("vwProvisionQuestion") }} pq on pq.ProvisionQuestion_QuestionId = qa.Answer_QuestionId
    ),
    all_ap_wide as (select * from {{ ref("vwAuthorityDirectHaileyProvision") }}),
    all_ap_long as (
        select
            Tenant_Id,
            Authority_Id,
            -- Related_AuthorityId,
            source_ap AuthorityProvision_Id,
            -- NULL target_ap
            'Direct' Related
        from all_ap_wide

        union all

        select
            Tenant_Id,
            -- Authority_Id,
            Related_AuthorityId Authority_Id,
            -- NULL source_ap,
            target_ap AuthorityProvision_Id,
            'Hailey' Related
        from all_ap_wide
    ),
    base_score as (
        select
            s.Template_TenantId,
            a.Tenant_Id,
            auth.Authority_Id,
            auth.Authority_Name,
            a.Related Authority_LinkType,
            s.Template_RelatedAuthorityId,
            s.Template_AuthorityRelation,
            s.Template_Id,
            s.Template_Name,
            s.Template_Version,
            s.Template_AssessmentStyle,
            --
            s.Assessment_Id,
            s.Assessment_Name,
            s.Assessment_TenantVendorId,
            s.Assessment_EngagementId,
            s.Answer_QuestionId,
            s.Question_TypeCode,
            s.Answer_IsCompleted,
            -- Raw Score
            s.Question_Weighting,
            s.Question_Multiplier,
            s.Answer_Id,
            s.Answer_RiskStatusCode,
            s.Answer_AnswerText,
            ap.AuthorityProvision_Id,
            ap.AuthorityProvision_ReferenceId,
            ap.AuthorityProvision_Name,
            -- Calculated scores
            s.Answer_RiskStatusCalc,
            s.Answer_WeightedScore,
            s.Answer_Score
        from assessment_score as s
        join
            all_ap_long as a
            on a.Authority_Id = s.Template_AuthorityId
            and a.AuthorityProvision_Id = s.ProvisionQuestion_AuthorityProvisionId
        join {{ ref("vwAuthority") }} as auth on auth.Authority_Id = a.Authority_Id
        join {{ ref("vwAuthorityProvision") }} as ap on ap.AuthorityProvision_Id = a.AuthorityProvision_Id
    )

select b.*
from
    base_score b

    /***** 
Test filters
where Authority_Id = 10 and auth_to_ass.TemplateId = 58 and TemplateVersion = 1 -- 98 rows 
*****/
    
