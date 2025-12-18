with answr as(
select
a.Uuid,
a.TenantId,
a.AssessmentResponseId AssessmentResponse_Id,
a.QuestionId Question_Id,
a.Id Answer_Id,
a.ComponentStr Answer_ComponentStr,
COALESCE(
JSON_VALUE(a.ComponentStr, '$.RadioCustom'),
REPLACE(REPLACE(REPLACE(REPLACE(JSON_QUERY(ComponentStr, '$.MultiSelectValues'), '[', ''), ']', ''), ',', ', '), '"', ''),
JSON_VALUE(ComponentStr, '$.TextArea')
) as Answer_Response,
case when JSON_VALUE(a.ComponentStr, '$.RadioCustom') is not null or 
REPLACE(REPLACE(REPLACE(REPLACE(JSON_QUERY(ComponentStr, '$.MultiSelectValues'), '[', ''), ']', ''), ',', ', '), '"', '') is not null 
then JSON_VALUE(ComponentStr, '$.TextArea') end Answer_Explanation,
a.Status Answer_StatusEnum,
case a.[Status]
when 1 then 'Published'
when 2 then 'In Progress'
when 3 then 'Submitted'
else 'Others' end Answer_Status,
a.RiskStatus Answer_RiskStatusEnum,
case
when a.RiskStatus = 0 then 'No risk'
when a.RiskStatus = 6 then 'Very low'
when a.RiskStatus = 1 then 'Low'
when a.RiskStatus = 3 then 'Medium'
when a.RiskStatus = 4 then 'High'
when a.RiskStatus = 5 then 'Very High'
else 'Undefined' end as Answer_RiskStatus,
a.Score Answer_Score,
a.MaxPossibleScore Answer_MaxPossibleScore,
a.Compliance Answer_ComplianceEnum,
case
when a.Compliance = 1 then 'Compliant'
when a.Compliance = 2 then 'Not compliant'
when a.Compliance = 3 then 'Partially compliant'
else 'None' end Answer_Compliance,
a.ResponderId Answer_ResponderId,
a.ReviewerComment Answer_Comments

from {{ source("assessment_ref_models", "Answer") }} a
where a.IsDeleted = 0

union all

select distinct
NULL Uuid,
q.TenantId,
qgr.AssessmentResponseId AssessmentResponse_Id,
qg.Id Question_Id,
NULL Answer_Id,
NULL Answer_ComponentStr,
NULL Answer_Response,
NULL Answer_Explanation,
NULL Answer_StatusEnum,
NULL Answer_Status,
NULL Answer_RiskStatusEnum,
NULL Answer_RiskStatus,
NULL Answer_Score,
NULL Answer_MaxPossibleScore,
qg.Compliance Answer_ComplianceEnum,
case
when qg.Compliance = 1 then 'Compliant'
when qg.Compliance = 2 then 'Not compliant'
when qg.Compliance = 3 then 'Partially compliant'
else 'None' end Answer_Compliance,
NULL Answer_ResponderId,
NULL Answer_Comments

from {{ source("assessment_ref_models", "Question") }} q
join {{ source("assessment_ref_models", "QuestionGroup") }} qg
on qg.TenantId = q.TenantId and qg.Id = q.QuestionGroupId and qg.IsDeleted = 0
left join {{ source("assessment_ref_models", "QuestionGroupResponse") }} qgr
on qgr.TenantId = qg.TenantId and qgr.QuestionGroupId = qg.Id and qgr.IsDeleted = 0
where q.IsDeleted = 0

union all

select distinct
NULL Uuid,
q.TenantId,
qgr.AssessmentResponseId AssessmentResponse_Id,
qgr.Id Question_Id,
NULL Answer_Id,
NULL Answer_ComponentStr,
NULL Answer_Response,
NULL Answer_Explanation,
NULL Answer_StatusEnum,
NULL Answer_Status,
NULL Answer_RiskStatusEnum,
NULL Answer_RiskStatus,
NULL Answer_Score,
NULL Answer_MaxPossibleScore,
qgr.Compliance Answer_ComplianceEnum,
case
when qgr.Compliance = 1 then 'Compliant'
when qgr.Compliance = 2 then 'Not compliant'
when qgr.Compliance = 3 then 'Partially compliant'
else 'None' end Answer_Compliance,
NULL Answer_ResponderId,
NULL Answer_Comments

from {{ source("assessment_ref_models", "Question") }} q
join {{ source("assessment_ref_models", "QuestionGroup") }} qg
on qg.TenantId = q.TenantId and qg.Id = q.QuestionGroupId and qg.IsDeleted = 0
join {{ source("assessment_ref_models", "QuestionGroupResponse") }} qgr
on qgr.TenantId = qg.TenantId and qgr.QuestionGroupId = qg.Id and qgr.IsDeleted = 0
where q.IsDeleted = 0
)

select
Uuid,
TenantId,
AssessmentResponse_Id,
Question_Id,
Answer_Id,
Answer_ComponentStr,
Answer_Response,
Answer_Explanation,
Answer_StatusEnum,
Answer_Status,
Answer_RiskStatusEnum,
Answer_RiskStatus,
Answer_Score,
Answer_MaxPossibleScore,
Answer_ComplianceEnum,
Answer_Compliance,
Answer_ResponderId,
Answer_Comments

from answr