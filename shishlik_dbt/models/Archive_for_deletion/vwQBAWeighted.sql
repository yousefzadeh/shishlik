select distinct
    at.Assessment_Name Template,
    tv.TenantVendor_Name,
    aa.Assessment_Name,
    aa.Assessment_TenantId,
    aa.Assessment_AssessmentVersionName,
    ad.AssessmentDomain_Name,
    ad.AssessmentDomain_ID,
    tpc.CustomFieldName,
    tpc.CustomFieldValue,
    aa.Assessment_QuestionTypeCode,
    aa.Assessment_IsDeprecatedAssessmentVersion,
    aa.Assessment_ResponseCompletedDate,
    aa.Assessment_Status,
    aa.Assessment_StatusCode
-- , m.*
-- , a.Answer_RadioCustom
-- , qtj.Tags_Name
from {{ ref("vwAssessment") }} aa
inner join {{ ref("vwAssessmentDomain") }} ad on (aa.Assessment_ID = ad.AssessmentDomain_AssessmentId)
inner join {{ ref("vwTenantVendor") }} tv on (aa.Assessment_TenantVendorId = tv.TenantVendor_Id)
inner join {{ ref("vwThirdPartyCustomTable") }} tpc on (tv.TenantVendor_Id = tpc.TenantVendor_Id)
inner join
    {{ ref("vwAssessment") }} at
    on (at.Assessment_ID = aa.Assessment_CreatedFromTemplateId)
    and (at.Assessment_IsTemplate = 1 and at.Assessment_WorkFlowId = 0)
-- join meth m
-- on m.AssessmentDomain_ID = ad.AssessmentDomain_ID 
-- left join "6clicks-dev-ihsopk"."test_dbt_cicd"."vwQuestion" q 
-- on q.Question_AssessmentDomainId = m.AssessmentDomain_ID
-- left join "6clicks-dev-ihsopk"."test_dbt_cicd"."vwAnswer" a 
-- on a.Answer_QuestionId = q.Question_ID 
-- left join "6clicks-dev-ihsopk"."test_dbt_cicd"."vwQuestionTagsJoined" qtj
-- on qtj.QuestionTags_QuestionId = q.Question_ID
where (aa.Assessment_IsTemplate = 0 and aa.Assessment_WorkFlowId = 0)
