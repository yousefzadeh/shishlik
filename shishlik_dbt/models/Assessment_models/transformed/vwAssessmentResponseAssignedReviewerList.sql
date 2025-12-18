{# 
    assigned reviewers -
        Assessment -> AssessmentDomain -> Question -> QuestionUser -> UserId (if user)
        Assessment -> AssessmentDomain -> Question -> QuestionUser -> OrganizationUnitId (if group)
        Assessment -> AssessmentDomain -> Question -> QuestionGroup -> QuestionUser -> UserId (if user) (for parent child question)
        Assessment -> AssessmentDomain -> Question -> QuestionGroup -> QuestionUser -> OrganizationUnitId (if group) (for parent child question)
        Assessment -> AssessmentDomain -> Question -> QuestionGroup -> QuestionGroupResponse -> QuestionUser -> UserId (if user) (for looped questions)
        Assessment -> AssessmentDomain -> Question -> QuestionGroup -> QuestionGroupResponse -> QuestionUser ->  OrganizationUnitId (if group) (for looped questions) 
#}
with
    simple_question as (
        select distinct 
            ar.AssessmentResponse_Id, 
            ar.AssessmentResponse_TenantId, 
            u.AbpUsers_FullName UserName,
            ar.AssessmentResponse_UpdateTime
        from {{ ref("vwAssessmentResponse") }} ar
        join {{ ref("vwAssessment") }} ass on ass.Assessment_Id = ar.AssessmentResponse_AssessmentId
        join {{ ref("vwAssessmentDomain") }} ad on ass.Assessment_Id = ad.AssessmentDomain_AssessmentId
        join {{ source("assessment_models","Question") }} q on ad.AssessmentDomain_Id = q.AssessmentDomainId
        join {{ source("assessment_models","QuestionUser") }} qu on qu.QuestionId = q.Id and qu.IsDeleted = 0
        join {{ ref("vwAbpUser") }} u on qu.UserId = u.AbpUsers_Id
        union all
        select distinct
            ar.AssessmentResponse_Id, 
            ar.AssessmentResponse_TenantId, 
            o.AbpOrganizationUnits_DisplayName UserName,
            ar.AssessmentResponse_UpdateTime
        from {{ ref("vwAssessmentResponse") }} ar
        join {{ ref("vwAssessment") }} ass on ass.Assessment_Id = ar.AssessmentResponse_AssessmentId
        join {{ ref("vwAssessmentDomain") }} ad on ass.Assessment_Id = ad.AssessmentDomain_AssessmentId
        join {{ source("assessment_models","Question") }} q on ad.AssessmentDomain_Id = q.AssessmentDomainId
        join {{ source("assessment_models","QuestionUser") }} qu on qu.QuestionId = q.Id and qu.IsDeleted = 0
        join {{ ref("vwAbpOrganizationUnits") }} o on qu.OrganizationUnitId = o.AbpOrganizationUnits_Id
    ),
    child_question as (
        select distinct 
            ar.AssessmentResponse_Id, 
            ar.AssessmentResponse_TenantId, 
            u.AbpUsers_FullName UserName,
            ar.AssessmentResponse_UpdateTime
        from {{ ref("vwAssessmentResponse") }} ar
        join {{ ref("vwAssessment") }} ass on ass.Assessment_Id = ar.AssessmentResponse_AssessmentId
        join {{ ref("vwAssessmentDomain") }} ad on ass.Assessment_Id = ad.AssessmentDomain_AssessmentId
        join {{ source("assessment_models","Question") }} q on ad.AssessmentDomain_Id = q.AssessmentDomainId
        join {{ source("assessment_models","QuestionUser") }} qu on qu.QuestionId = q.Id and qu.IsDeleted = 0
        join {{ ref("vwAbpUser") }} u on qu.UserId = u.AbpUsers_Id
        join {{ source("assessment_models","QuestionGroup") }} qg on q.QuestionGroupId = qg.Id and qg. [Type] = 11
        union all
        select distinct
            ar.AssessmentResponse_Id, 
            ar.AssessmentResponse_TenantId, 
            o.AbpOrganizationUnits_DisplayName UserName,
            ar.AssessmentResponse_UpdateTime
        from {{ ref("vwAssessmentResponse") }} ar
        join {{ ref("vwAssessment") }} ass on ass.Assessment_Id = ar.AssessmentResponse_AssessmentId
        join {{ ref("vwAssessmentDomain") }} ad on ass.Assessment_Id = ad.AssessmentDomain_AssessmentId
        join {{ source("assessment_models","Question") }} q on ad.AssessmentDomain_Id = q.AssessmentDomainId
        join {{ source("assessment_models","QuestionUser") }} qu on qu.QuestionId = q.Id and qu.IsDeleted = 0
        join {{ ref("vwAbpOrganizationUnits") }} o on qu.OrganizationUnitId = o.AbpOrganizationUnits_Id
        join {{ source("assessment_models","QuestionGroup") }} qg on q.QuestionGroupId = qg.Id and qg. [Type] = 11
    ),
    loop_question as (
        select distinct 
            ar.AssessmentResponse_Id, 
            ar.AssessmentResponse_TenantId, 
            u.AbpUsers_FullName UserName,
            ar.AssessmentResponse_UpdateTime
        from {{ ref("vwAssessmentResponse") }} ar
        join {{ ref("vwAssessment") }} ass on ass.Assessment_Id = ar.AssessmentResponse_AssessmentId
        join {{ ref("vwAssessmentDomain") }} ad on ass.Assessment_Id = ad.AssessmentDomain_AssessmentId
        join {{ source("assessment_models","Question") }} q on ad.AssessmentDomain_Id = q.AssessmentDomainId
        join {{ source("assessment_models","QuestionUser") }} qu on qu.QuestionId = q.Id and qu.IsDeleted = 0
        join {{ ref("vwAbpUser") }} u on qu.UserId = u.AbpUsers_Id
        join {{ source("assessment_models","QuestionGroup") }} qg on q.QuestionGroupId = qg.Id and qg. [Type] = 12
        union all
        select distinct
            ar.AssessmentResponse_Id, 
            ar.AssessmentResponse_TenantId, 
            o.AbpOrganizationUnits_DisplayName UserName,
            ar.AssessmentResponse_UpdateTime
        from {{ ref("vwAssessmentResponse") }} ar
        join {{ ref("vwAssessment") }} ass on ass.Assessment_Id = ar.AssessmentResponse_AssessmentId
        join {{ ref("vwAssessmentDomain") }} ad on ass.Assessment_Id = ad.AssessmentDomain_AssessmentId
        join {{ source("assessment_models","Question") }} q on ad.AssessmentDomain_Id = q.AssessmentDomainId
        join {{ source("assessment_models","QuestionUser") }} qu on qu.QuestionId = q.Id and qu.IsDeleted = 0
        join {{ ref("vwAbpOrganizationUnits") }} o on qu.OrganizationUnitId = o.AbpOrganizationUnits_Id
        join {{ source("assessment_models","QuestionGroup") }} qg on q.QuestionGroupId = qg.Id and qg. [Type] = 12
    ),
    all_question as (
        select distinct *
        from
            (
                select *
                from simple_question
                union all
                select *
                from child_question
                union all
                select *
                from loop_question
            ) as T
    ),
    final as (
        select 
            AssessmentResponse_TenantId, 
            AssessmentResponse_Id, 
            string_agg(UserName, ', ') as UserList,
            max(AssessmentResponse_UpdateTime) as AssignedReviewerList_UpdateTime
        from all_question
        group by AssessmentResponse_TenantId, AssessmentResponse_Id
    )
select *
from
    final
