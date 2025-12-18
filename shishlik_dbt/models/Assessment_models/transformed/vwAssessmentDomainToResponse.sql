select
    ad.AssessmentDomain_AssessmentId as AssessmentId,
    ad.AssessmentDomain_Name,
    ad.AssessmentDomain_IntroductionText AssessmentDomain_Description,
    ad.AssessmentDomain_Order,
    ad.AssessmentDomain_TenantId,
    ad.AssessmentDomain_Id,
    ar.AssessmentResponse_Id,
    ar.AssessmentResponse_Name,
    ar.AssessmentResponse_Status,
    ar.AssessmentResponse_StatusCode,
    ar.AssessmentResponse_Version,
    ar.AssessmentResponse_UserId,
    ar.AssessmentResponse_RiskRatingWeightedScore, 
    case when coalesce(ar.AssessmentResponse_UpdateTime, '2000-01-01 00:00:01.000') <= ad.AssessmentDomain_UpdateTime then ad.AssessmentDomain_UpdateTime
        else ar.AssessmentResponse_UpdateTime  --Picking the most recent date columns wrt AssessmentDomain and AssessmentResponse updates
        end as ADR_UpdateTime
from {{ ref("vwAssessmentDomain") }} ad
left join
    {{ ref("vwAssessmentResponse") }} ar
    on ad.AssessmentDomain_AssessmentId = ar.AssessmentResponse_AssessmentId
    and ad.AssessmentDomain_TenantId = ar.AssessmentResponse_TenantId
