with
    access_member as (
        select AssessmentAccessMember_AssessmentId
        , AccessMemberName
        , AssessmentAccessMember_UpdateTime
        from {{ ref("vwAssessmentAccessMemberFilter") }} a
    ),
    list as (
        select AssessmentAccessMember_AssessmentId, string_agg(AccessMemberName, ', ') as AccessMemberList
        , max(AssessmentAccessMember_UpdateTime) as AssessmentAccessMember_UpdateTime
        from access_member
        group by AssessmentAccessMember_AssessmentId
    )
select *
from list
