with quest as (
select
q.Uuid,
q.TenantId,
q.AssessmentDomainId AssessmentDomain_Id,
q.Id Question_Id,
q.IdRef Question_IdRef,
q.Name Question_Name,
q.[Description] Question_Description,
cast (q.[Order] as varchar(4)) Question_Order,
q.Type Question_TypeId,
case when [Type] = 1 then 'Yes No'
when [Type] in (2, 5, 6, 10) then 'Choose One'
when [Type] in (3, 7, 8) then 'Choose Many'
when [Type] in (4, 9) then 'Free Text Response'
else 'Undefined' end as Question_Type,
q.VendorDocumentRequired Question_DocumentUploadMandatory,
q.HasConditionalLogic Question_SkipCondition,
q.IsMandatory Question_IsMandatory,
q.IsMultiSelectType Question_IsMultiSelectType

from {{ source("assessment_ref_models", "Question") }} q
where q.IsDeleted = 0
and q.IdRef is not null

union all

select distinct
NULL Uuid,
qg.TenantId,
q.AssessmentDomainId AssessmentDomain_Id,
qg.Id Question_Id,
case
when qg.Type = 11 then SUBSTRING(q.IdRef, 1, 3)
else SUBSTRING(qgr.IdRef, 1, 3) end Question_IdRef,
qg.Name Question_Name,
qg.[Description] Question_Description,
NULL Question_Order,
qg.[Type] Question_TypeId,
case when qg.[Type] = 11 then 'Parent/Child'
when qg.[Type] = 12 then 'Looped question'
else 'Undefined' end as Question_Type,
NULL Question_DocumentUploadMandatory,
NULL Question_SkipCondition,
NULL Question_IsMandatory,
NULL Question_IsMultiSelectType

from {{ source("assessment_ref_models", "Question") }} q
join {{ source("assessment_ref_models", "QuestionGroup") }} qg
on qg.TenantId = q.TenantId and qg.Id = q.QuestionGroupId and qg.IsDeleted = 0
left join {{ source("assessment_ref_models", "QuestionGroupResponse") }} qgr
on qgr.TenantId = qg.TenantId and qgr.QuestionGroupId = qg.Id and qgr.IsDeleted = 0
where q.IsDeleted = 0

union all

select distinct
NULL Uuid,
qg.TenantId,
q.AssessmentDomainId AssessmentDomain_Id,
qgr.Id Question_Id,
qgr.IdRef Question_IdRef,
qgr.Response Question_Name,
NULL Question_Description,
qgr.[Order] Question_Order,
qg.[Type] Question_TypeId,
case when qg.[Type] = 11 then 'Parent/Child'
when qg.[Type] = 12 then 'Looped question'
else 'Undefined' end as Question_Type,
NULL Question_DocumentUploadMandatory,
NULL Question_SkipCondition,
NULL Question_IsMandatory,
NULL Question_IsMultiSelectType

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
AssessmentDomain_Id,
Question_Id,
Question_IdRef,
Question_Name,
Question_Description,
Question_Order,
Question_TypeId,
Question_Type,
Question_DocumentUploadMandatory,
Question_SkipCondition,
Question_IsMandatory,
Question_IsMultiSelectType

from quest