select
    qwm.*,
    qw.Assessment_Name Template,
    qw.TenantVendor_Name,
    qw.Assessment_Name,
    qw.Assessment_TenantId,
    qw.Assessment_AssessmentVersionName,
    -- qw.AssessmentDomain_Name,
    qw.CustomFieldName,
    qw.CustomFieldValue,
    qw.Assessment_QuestionTypeCode,
    qw.Assessment_IsDeprecatedAssessmentVersion,
    qw.Assessment_ResponseCompletedDate,
    qw.Assessment_Status,
    qw.Assessment_StatusCode
from {{ ref("vwQBAWeighted") }} qw
left join {{ ref("vwQBAWeightedMetrics") }} qwm on qwm.AssessmentDomain_ID = qw.AssessmentDomain_ID
