with
    q as (
        select
            ar.Id AssessmentResponseId,
            ar.TenantId,
            ar.AssessmentId,
            ar.SubmittedDate,
			cast(coalesce(ar.LastModificationTime,ar.CreationTime) as datetime2) as UpdateTime,
            ROW_NUMBER() over (partition by ar.TenantId, ass.RootAssessmentId order by ar.SubmittedDate desc) recent_seq
        from {{ source("assessment_models","Assessment") }} ass
        join {{ source("assessment_models","AssessmentResponse") }} ar on ass.Id = ar.AssessmentId
    )
select TenantId,
        AssessmentResponseId LatestSubmmitedAssessmentResponseId,
        UpdateTime as LatestAssessmentResponse_UpdateTime
from q
where   
    recent_seq = 1 
    and SubmittedDate is not NULL
