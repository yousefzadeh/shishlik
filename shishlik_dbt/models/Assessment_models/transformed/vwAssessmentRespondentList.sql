with AssessmentResponse as (
    select distinct Id,
        UserId,
		cast(coalesce(LastModificationTime,CreationTime) as datetime2) as UpdateTime
        from {{ source("assessment_models","AssessmentResponse") }}
    ),
    assessmentresponse_respondent as (
        select distinct ar.Id,
        AbpUsers_FullName,
		ar.UpdateTime
        from AssessmentResponse ar
        join {{ ref("vwAbpUser") }} u on ar.UserId = u.AbpUsers_Id
    ),
    answer_respondent as (
        select distinct ar.Id, 
        AbpUsers_FullName,
		ar.UpdateTime
        from AssessmentResponse ar
        join {{ ref("vwAnswer") }} a on a.Answer_AssessmentResponseId = ar.Id
        join {{ ref("vwAbpUser") }} u on a.Answer_ResponderId = u.AbpUsers_Id
    ),
    answer_creator as (
        select distinct ar.Id,
        AbpUsers_FullName,
		ar.UpdateTime
        from AssessmentResponse ar
        join {{ ref("vwAnswer") }} a on a.Answer_AssessmentResponseId = ar.Id
        join {{ ref("vwAbpUser") }} u on a.Answer_CreatorUserId = u.AbpUsers_Id
    ),
    un as (
        select distinct Id, 
        AbpUsers_FullName, 
        UpdateTime
        from ( 
            select * from assessmentresponse_respondent 
            union all
            select * from answer_respondent 
            union all 
            select * from answer_creator 
            ) a
    ),
    list as (
        select 
        Id AssessmentResponse_Id,
         string_agg(AbpUsers_FullName, ', ') as RespondentList,
         max(UpdateTime) as AssessmentResponseList_UpdateTime
        from un 
        group by Id
    )
select 
AssessmentResponse_Id,
RespondentList,
AssessmentResponseList_UpdateTime
from list

