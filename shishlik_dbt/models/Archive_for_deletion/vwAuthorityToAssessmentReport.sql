{{ config(materialized="view") }}

with
    score_detail as (

        select distinct
            Template_TenantId,
            Authority_Id,
            Authority_Name,
            Template_Id,
            Template_Name,
            Template_Version,
            Template_AssessmentStyle,
            Assessment_Id,
            Assessment_Name,
            Assessment_TenantVendorId,
            Assessment_EngagementId,
            AuthorityProvision_Id,
            AuthorityProvision_ReferenceId,
            AuthorityProvision_Name,
            Answer_QuestionId,
            Question_TypeCode,
            Question_Weighting,
            Answer_Score,
            Answer_IsCompleted
        from {{ ref("vwAuthorityToAssessmentDetail") }}
        where Assessment_Id is not NULL
    ),
    ass_score as (
        {#
    Roll up for Assessment Score

    Risk Rated assessments have scores and labels
 #}
        select
            AuthorityProvision_Id AssessmentScore_ProvisionId,
            Assessment_Id AssessmentScore_AssessmentId,
            min(Answer_Score) ass_min_score,
            max(Answer_Score) ass_max_score,
            avg(Answer_Score) ass_avg_score,
            case
                when round(min(Answer_Score), 0) = 0
                then 'No Risk'
                when round(min(Answer_Score), 0) = 1
                then 'Very Low Risk'
                when round(min(Answer_Score), 0) = 2
                then 'Low Risk'
                when round(min(Answer_Score), 0) = 3
                then 'Medium Risk'
                when round(min(Answer_Score), 0) = 4
                then 'High Risk'
                when round(min(Answer_Score), 0) = 5
                then 'Very High Risk'
                else 'Undefined'
            end as ass_min_score_label,
            case
                when round(max(Answer_Score), 0) = 0
                then 'No Risk'
                when round(max(Answer_Score), 0) = 1
                then 'Very Low Risk'
                when round(max(Answer_Score), 0) = 2
                then 'Low Risk'
                when round(max(Answer_Score), 0) = 3
                then 'Medium Risk'
                when round(max(Answer_Score), 0) = 4
                then 'High Risk'
                when round(max(Answer_Score), 0) = 5
                then 'Very High Risk'
                else 'Undefined'
            end as ass_max_score_label,
            case
                when round(avg(Answer_Score), 0) = 0
                then 'No Risk'
                when round(avg(Answer_Score), 0) = 1
                then 'Very Low Risk'
                when round(avg(Answer_Score), 0) = 2
                then 'Low Risk'
                when round(avg(Answer_Score), 0) = 3
                then 'Medium Risk'
                when round(avg(Answer_Score), 0) = 4
                then 'High Risk'
                when round(avg(Answer_Score), 0) = 5
                then 'Very High Risk'
                else 'Undefined'
            end as ass_avg_score_label
        from score_detail
        where Template_AssessmentStyle = 'Risk Rated'
        group by AuthorityProvision_Id, Assessment_Id

        union all

        {# Weighted score has no labels #}
        select
            AuthorityProvision_Id AssessmentScore_ProvisionId,
            Assessment_Id,
            min(Answer_Score) ass_min_score,
            max(Answer_Score) ass_max_score,
            avg(Answer_Score) ass_avg_score,
            'Not Risk Rated' as ass_min_score_label,
            'Not Risk Rated' as ass_max_score_label,
            'Not Risk Rated' as ass_avg_score_label
        from score_detail
        where Template_AssessmentStyle <> 'Risk Rated'
        group by AuthorityProvision_Id, Assessment_Id

    ),
    prov_score as (
        {# 
    Roll up at Provision level

	Risk Rated Score with Label
 #}
        select
            score_detail.AuthorityProvision_Id ProvisionScore_ProvisionId,
            score_detail.Template_Id ProvisionScore_TemplateId,
            min(Answer_Score) prov_min_score,
            max(Answer_Score) prov_max_score,
            avg(Answer_Score) prov_avg_score,
            case
                when round(min(Answer_Score), 0) = 0
                then 'No Risk'
                when round(min(Answer_Score), 0) = 1
                then 'Very Low Risk'
                when round(min(Answer_Score), 0) = 2
                then 'Low Risk'
                when round(min(Answer_Score), 0) = 3
                then 'Medium Risk'
                when round(min(Answer_Score), 0) = 4
                then 'High Risk'
                when round(min(Answer_Score), 0) = 5
                then 'Very High Risk'
                else 'Undefined'
            end as prov_min_score_label,
            case
                when round(max(Answer_Score), 0) = 0
                then 'No Risk'
                when round(max(Answer_Score), 0) = 1
                then 'Very Low Risk'
                when round(max(Answer_Score), 0) = 2
                then 'Low Risk'
                when round(max(Answer_Score), 0) = 3
                then 'Medium Risk'
                when round(max(Answer_Score), 0) = 4
                then 'High Risk'
                when round(max(Answer_Score), 0) = 5
                then 'Very High Risk'
                else 'Undefined'
            end as prov_max_score_label,
            case
                when round(avg(Answer_Score), 0) = 0
                then 'No Risk'
                when round(avg(Answer_Score), 0) = 1
                then 'Very Low Risk'
                when round(avg(Answer_Score), 0) = 2
                then 'Low Risk'
                when round(avg(Answer_Score), 0) = 3
                then 'Medium Risk'
                when round(avg(Answer_Score), 0) = 4
                then 'High Risk'
                when round(avg(Answer_Score), 0) = 5
                then 'Very High Risk'
                else 'Undefined'
            end as prov_avg_score_label
        from score_detail
        where score_detail.Template_AssessmentStyle = 'Risk Rated'
        group by score_detail.AuthorityProvision_Id, score_detail.Template_Id

        union all

        {# 
	 	Weighted Score has no labels
	  #}
        select
            score_detail.AuthorityProvision_Id ProvisionScore_ProvisionId,
            score_detail.Template_Id ProvisionScore_TemplateId,
            min(Answer_Score) prov_min_score,
            max(Answer_Score) prov_max_score,
            avg(Answer_Score) prov_avg_score,
            'Not Risk Rated' as prov_min_score_label,
            'Not Risk Rated' as prov_max_score_label,
            'Not Risk Rated' as prov_avg_score_label
        from score_detail
        where score_detail.Template_AssessmentStyle = 'Weighted Score'
        group by score_detail.AuthorityProvision_Id, score_detail.Template_Id

    ),
    vendor as (

        select tv.TenantVendor_Id, tv.TenantVendor_Name from {{ ref("vwTenantVendor") }} tv

    {# select 
e.Engagement_Id,
e.Engagement_Name,
e.Engagement_Type,
e.Engagement_Description,
e.Engagement_BusinessUnit,
tv.TenantVendor_Id,
t.AbpTenants_Name VendorName
from   ref("vwEngagement") }} e
join   ref("vwTenantVendor") }} tv
  on e.Engagement_TenantVendorId = tv.TenantVendor_Id
join   ref("vwAbpTenants") }} t
  on tv.TenantVendor_VendorId = t.AbpTenants_Id #}
    )
{# 
    Replicated Report
 #}
select distinct
    score_detail.Template_TenantId,
    score_detail.Authority_Id,
    score_detail.Authority_Name,
    score_detail.Template_Id,
    score_detail.Template_Name,
    score_detail.Template_Version,
    score_detail.Template_AssessmentStyle,
    score_detail.Assessment_Id,
    score_detail.Assessment_Name,
    score_detail.AuthorityProvision_Id,
    score_detail.AuthorityProvision_ReferenceId,
    score_detail.AuthorityProvision_Name,
    custom.AuthorityProvisionCustomValue_FieldName,
    custom.AuthorityProvisionCustomValue_Value,
    vendor.*,
    ass_score.*,
    prov_score.*
from score_detail
left join
    {{ ref("vwAuthorityProvisionCustomValue") }} as custom
    on score_detail.Authority_Id = custom.AuthorityProvisionCustomValue_AuthorityId
    and score_detail.AuthorityProvision_Id = custom.AuthorityProvisionCustomValue_AuthorityProvisionId
    and custom.AuthorityProvisionCustomValue_FieldOrder = 1  -- only 1 column
left join
    vendor
    -- on score_detail.Assessment_EngagementId = vendor.Engagement_Id
    on score_detail.Assessment_TenantVendorId = vendor.TenantVendor_Id
left join
    ass_score
    on score_detail.Assessment_Id = ass_score.AssessmentScore_AssessmentId
    and score_detail.AuthorityProvision_Id = ass_score.AssessmentScore_ProvisionId
left join
    prov_score
    on score_detail.AuthorityProvision_Id = prov_score.ProvisionScore_ProvisionId
    and score_detail.Template_Id = prov_score.ProvisionScore_TemplateId

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
    
