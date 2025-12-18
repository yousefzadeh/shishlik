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
(AuthorityId) Authority Name
Assessment Template Name
Assessment Template Version
Vendor
(AssessmentId) Assessment Name
RollUp method (Fixed options 'Min', 'Max', Average)
Actual Result

Display:
Style: Crosstab
Crosstab Columns: #3 Assessments
Crosstab Rows:    #4 Provision
Metric: 
If TemplateAssessmentStyle is "Risk Rated" then "score labels"
If TemplateAssessmentStyle is "Weighted Score" then "score"
The metric is aggregated 3 ways depending on the filter value of the Rollup Method (min, max, average)

Provision Custom fields:
In the existing report, only the first column (CustomFieldOrder=1) is shown as a column with the column name as CustomFieldName and value as CustomValue
In YF, all custom fields may be shown as a popup drill through.

 #}
with
    tpl as (
        {# Assessment Template #}
        select *
        from {{ ref("vwAssessment") }} a
        where a.Assessment_IsTemplate = 1 and a.Assessment_IsArchived = 0 and a.Assessment_Status in (3, 100)
    ),
    ass as (
        {# Assessment #}
        select *
        from {{ ref("vwAssessment") }} a
        where
            a.Assessment_WorkFlow = 'Question'  -- QBA only
            and a.Assessment_IsTemplate = 0  -- assessments only
            and a.Assessment_IsArchived = 0
            and a.Assessment_IsDeprecatedAssessmentVersion = 0
            and a.Assessment_Status in (4, 5, 6)  -- completed
    ),
    custom_value as (
        {# 
  Custom table view to extract column and values from JSON string in AuthorityProvision table
  All AuthorityProvision_Id will be listed
  Where no Custom field found, 1 column, default name and value will be returned
 #}
        select * from {{ ref("vwAuthorityProvisionCustomValue") }}
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
            tpl.Assessment_TenantId Template_TenantId,
            a.Authority_Id,
            a.Authority_Name,
            tpl.Assessment_Id TemplateId,
            tpl.Assessment_Name TemplateName,
            tpl.Assessment_TemplateVersion TemplateVersion,
            tpl.Assessment_QuestionTypeCode TemplateAssessmentStyle,
            ass.Assessment_Id,
            ass.Assessment_Name,
            ap.AuthorityProvision_Id,
            ap.AuthorityProvision_ReferenceId,
            ap.AuthorityProvision_Name
        from {{ ref("vwAuthorityProvision") }} ap
        join {{ ref("vwAuthority") }} a on ap.AuthorityProvision_AuthorityId = a.Authority_Id
        left join tpl on tpl.Assessment_AuthorityId = a.Authority_Id
        left join ass on ass.Assessment_CreatedFromTemplateId = tpl.Assessment_Id
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

  To Do: 
  Left join from Provision to questionAnswer?
  Can there be Assessment/Provision without any questions? 
#}
        select
            ass.Assessment_TenantId,
            ass.Assessment_CreatedFromTemplateId,
            ass.Assessment_Id,
            pq.ProvisionQuestion_AuthorityProvisionId,
            qa.Question_ID,
            ass.Assessment_Name,
            ass.Assessment_QuestionType,
            ass.Assessment_StatusCode,
            ass.Assessment_QuestionTypeCode,
            qa.Question_TypeCode,
            qa.Question_Weighting,
            qa.completed_flag,
            qa.Answer_Score,
            qa.Question_Weighting * qa.Answer_Score as WeightedScore,
            qa.Answer_RiskStatusCalc  -- Used for the score on risk based assessment type
        from ass
        join {{ ref("vwAssessmentDomain") }} ad on ad.AssessmentDomain_AssessmentId = ass.Assessment_Id
        join {{ ref("vwQuestionAnswer") }} qa on qa.Question_AssessmentDomainId = ad.AssessmentDomain_ID
        join {{ ref("vwProvisionQuestion") }} pq on pq.ProvisionQuestion_QuestionId = qa.Question_ID
    ),
    ass_score_detail as (
        {# 
    Scores are calculated differently for Risk Rated and Weighted score
 #}
        select
            ProvisionQuestion_AuthorityProvisionId,
            Assessment_TenantId,
            Assessment_Id,
            Assessment_Name,
            Assessment_QuestionTypeCode,
            Assessment_CreatedFromTemplateId,
            Question_Id,
            Question_TypeCode,
            Question_Weighting,
            Answer_Score,
            completed_flag,
            WeightedScore score
        from score
        where score.Assessment_QuestionType = 1  -- Weighted score

        union all

        select
            ProvisionQuestion_AuthorityProvisionId,
            Assessment_TenantId,
            Assessment_Id,
            Assessment_Name,
            Assessment_QuestionTypeCode,
            Assessment_CreatedFromTemplateId,
            Question_Id,
            Question_TypeCode,
            Question_Weighting,
            Answer_Score,
            completed_flag,
            Answer_RiskStatusCalc score
        from score
        where score.Assessment_QuestionType = 2  -- Risk rated
    ),
    ass_score as (
        {#
    Roll up for Assessment Score

    Risk Rated assessments have scores and labels
 #}
        select
            ProvisionQuestion_AuthorityProvisionId AssessmentScore_ProvisionId,
            Assessment_Id AssessmentScore_AssessmentId,
            min(score) ass_min_score,
            max(score) ass_max_score,
            avg(score) ass_avg_score,
            case
                when round(min(score), 0) = 0
                then 'No Risk'
                when round(min(score), 0) = 1
                then 'Very Low Risk'
                when round(min(score), 0) = 2
                then 'Low Risk'
                when round(min(score), 0) = 3
                then 'Medium Risk'
                when round(min(score), 0) = 4
                then 'High Risk'
                when round(min(score), 0) = 5
                then 'Very High Risk'
                else 'Undefined'
            end as ass_min_score_label,
            case
                when round(max(score), 0) = 0
                then 'No Risk'
                when round(max(score), 0) = 1
                then 'Very Low Risk'
                when round(max(score), 0) = 2
                then 'Low Risk'
                when round(max(score), 0) = 3
                then 'Medium Risk'
                when round(max(score), 0) = 4
                then 'High Risk'
                when round(max(score), 0) = 5
                then 'Very High Risk'
                else 'Undefined'
            end as ass_max_score_label,
            case
                when round(avg(score), 0) = 0
                then 'No Risk'
                when round(avg(score), 0) = 1
                then 'Very Low Risk'
                when round(avg(score), 0) = 2
                then 'Low Risk'
                when round(avg(score), 0) = 3
                then 'Medium Risk'
                when round(avg(score), 0) = 4
                then 'High Risk'
                when round(avg(score), 0) = 5
                then 'Very High Risk'
                else 'Undefined'
            end as ass_avg_score_label
        from ass_score_detail
        where Assessment_QuestionTypeCode = 'Risk Rated'
        group by ProvisionQuestion_AuthorityProvisionId, Assessment_Id

        union all

        {# Weighted score has no labels #}
        select
            ProvisionQuestion_AuthorityProvisionId AssessmentScore_ProvisionId,
            Assessment_Id,
            min(score) ass_min_score,
            max(score) ass_max_score,
            avg(score) ass_avg_score,
            'Not Risk Rated' as ass_min_score_label,
            'Not Risk Rated' as ass_max_score_label,
            'Not Risk Rated' as ass_avg_score_label
        from ass_score_detail
        where Assessment_QuestionTypeCode <> 'Risk Rated'
        group by ProvisionQuestion_AuthorityProvisionId, Assessment_Id

    ),
    prov_score as (
        {# 
    Roll up at Provision level
 #}
        select
            pq.ProvisionQuestion_AuthorityProvisionId ProvisionScore_ProvisionId,
            tpl.Assessment_Id ProvisionScore_TemplateId,
            min(score) prov_min_score,
            max(score) prov_max_score,
            avg(score) prov_avg_score,
            case
                when round(min(score), 0) = 0
                then 'No Risk'
                when round(min(score), 0) = 1
                then 'Very Low Risk'
                when round(min(score), 0) = 2
                then 'Low Risk'
                when round(min(score), 0) = 3
                then 'Medium Risk'
                when round(min(score), 0) = 4
                then 'High Risk'
                when round(min(score), 0) = 5
                then 'Very High Risk'
                else 'Undefined'
            end as prov_min_score_label,
            case
                when round(max(score), 0) = 0
                then 'No Risk'
                when round(max(score), 0) = 1
                then 'Very Low Risk'
                when round(max(score), 0) = 2
                then 'Low Risk'
                when round(max(score), 0) = 3
                then 'Medium Risk'
                when round(max(score), 0) = 4
                then 'High Risk'
                when round(max(score), 0) = 5
                then 'Very High Risk'
                else 'Undefined'
            end as prov_max_score_label,
            case
                when round(avg(score), 0) = 0
                then 'No Risk'
                when round(avg(score), 0) = 1
                then 'Very Low Risk'
                when round(avg(score), 0) = 2
                then 'Low Risk'
                when round(avg(score), 0) = 3
                then 'Medium Risk'
                when round(avg(score), 0) = 4
                then 'High Risk'
                when round(avg(score), 0) = 5
                then 'Very High Risk'
                else 'Undefined'
            end as prov_avg_score_label
        from ass_score_detail
        join {{ ref("vwProvisionQuestion") }} pq on ass_score_detail.Question_Id = pq.ProvisionQuestion_QuestionId
        join tpl on ass_score_detail.Assessment_CreatedFromTemplateId = tpl.Assessment_Id
        -- and ass_score_detail.Assessment_TenantId = tpl.Assessment_TenantId -- must be same tenant as template
        where ass_score_detail.Assessment_QuestionTypeCode = 'Risk Rated'
        group by pq.ProvisionQuestion_AuthorityProvisionId, tpl.Assessment_Id

        union all

        select
            pq.ProvisionQuestion_AuthorityProvisionId ProvisionScore_ProvisionId,
            tpl.Assessment_Id ProvisionScore_TemplateId,
            min(score) prov_min_score,
            max(score) prov_max_score,
            avg(score) prov_avg_score,
            'Not Risk Rated' as prov_min_score_label,
            'Not Risk Rated' as prov_max_score_label,
            'Not Risk Rated' as prov_avg_score_label
        from ass_score_detail
        join {{ ref("vwProvisionQuestion") }} pq on ass_score_detail.Question_Id = pq.ProvisionQuestion_QuestionId
        join tpl on ass_score_detail.Assessment_CreatedFromTemplateId = tpl.Assessment_Id
        where ass_score_detail.Assessment_QuestionTypeCode <> 'Risk Rated'
        group by pq.ProvisionQuestion_AuthorityProvisionId, tpl.Assessment_Id

    )

{# 
    Replicated Report
 #}
select distinct
    auth_to_ass.*,
    custom_value.AuthorityProvisionCustomValue_FieldName,
    custom_value.AuthorityProvisionCustomValue_Value,
    ass_score.*,
    prov_score.*
from auth_to_ass
join
    custom_value
    on auth_to_ass.Authority_Id = custom_value.AuthorityProvisionCustomValue_AuthorityId
    and auth_to_ass.AuthorityProvision_Id = custom_value.AuthorityProvisionCustomValue_AuthorityProvisionId
join
    ass_score
    on auth_to_ass.Assessment_Id = ass_score.AssessmentScore_AssessmentId
    and auth_to_ass.AuthorityProvision_Id = ass_score.AssessmentScore_ProvisionId
join
    prov_score
    on auth_to_ass.AuthorityProvision_Id = prov_score.ProvisionScore_ProvisionId
    and auth_to_ass.TemplateId = prov_score.ProvisionScore_TemplateId
where
    custom_value.AuthorityProvisionCustomValue_FieldOrder = 1
    -- Maintain grain to have 1 custom field to show only
    -- Release this constraint when YF is doing the drill thru on custom field
    {# 
Test filters
where Authority_Id = 10 
and auth_to_ass.TemplateId = 58 
and TemplateVersion = 1 -- 98 rows 
#}
    {# 
    Yellowfin view
    
    select *
    from auth_to_ass
    join ass_score_detail
      on auth_to_ass.Assessment_Id = ass_score_detail.Assessment_Id

 #}
    
