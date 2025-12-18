with
    owner as (
        select DISTINCT 
        AssessmentOwner_AssessmentId,
         OwnerName,
         AssessmentOwner_UpdateTime 
         from {{ ref("vwAssessmentOwnerFilter") }} a
    ),
    list as (
        select AssessmentOwner_AssessmentId, 
        string_agg(OwnerName, ', ') as OwnerList,
        max(AssessmentOwner_UpdateTime) as AssessmentOwnerList_UpdateTime
        from owner
        group by AssessmentOwner_AssessmentId
    )
select *
from list
