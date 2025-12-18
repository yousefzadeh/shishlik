select
r.Uuid,
case when s.IsShared = 0 then r.TenantId else e.DestinationTenantId end TenantId,
case when s.IsShared = 0 then abp.Name else abp2.Name end TenantName,
r.Id Risk_Id,
r.CreationTime Risk_CreationTime,
r.CreatorUserId Risk_CreatorUserId,
r.LastModificationTime Risk_LastModificationTime,
r.LastModifierUserId Risk_LastModifierUserId,
r.Name Risk_Name,
r.Description Risk_Description,
r.AbstractRiskId Risk_AbstractRiskId,
r.RiskReviewId Risk_RiskReviewId,
r.CommonCause Risk_CommonCause,
r.LikelyImpact Risk_LikelyImpact,
r.FavouriteRiskAssessmentId Risk_FavouriteRiskAssessmentId,
r.TenantEntityUniqueId Risk_IdRef,
r.WorkflowStageId Risk_WorkflowStageId,
case when wfs.Name is NULL then 'Unassigned' else wfs.Name end as Risk_WorkflowStage,
r.TreatmentDecisionId Risk_TreatmentDecisionId,
sl.Name Risk_TreatmentDecision,
r.TreatmentStatusId Risk_TreatmentStatusId,
case
when r.TreatmentStatusId = 1 then 'Draft'
when r.TreatmentStatusId = 2 then 'Approved'
when r.TreatmentStatusId = 3 then 'Treatment in progress'
when r.TreatmentStatusId = 4 then 'Treatment paused'
when r.TreatmentStatusId = 5 then 'Treatment cancelled'
when r.TreatmentStatusId = 6 then 'Treatment completed'
when r.TreatmentStatusId = 7 then 'Closed'
else 'Undefined' end Risk_TreatmentStatus,
r.RiskRatingId Risk_RiskRatingId,
case when r.RiskRatingId is null then 'No Risk' else tpa.Label end Risk_Rating,
r.IsArchived Risk_IsArchived,
s.IsShared Risk_IsShared

from {{ source("risk_ref_models", "Risk") }} r
join {{ source("abp_ref_models", "AbpTenants") }} abp
on abp.Id = r.TenantId
left join {{ source("miscellaneous_ref_models", "WorkflowStage") }} wfs
on wfs.Id = r.WorkflowStageId and wfs.IsDeleted = 0
left join {{ source("risk_ref_models", "StatusLists") }} sl
on sl.Id = r.TreatmentDecisionId and sl.IsDeleted = 0
left join {{ source("third-party_ref_models", "ThirdPartyAttributes") }} tpa
on tpa.Id = r.RiskRatingId and tpa.IsDeleted = 0
cross apply (values (0), (1)) AS s(IsShared)
left join {{ source("miscellaneous_ref_models", "EntityShareLog") }} e
on s.IsShared = 1
and e.TenantId = r.TenantId
and e.SourceEntityId = r.Id
and e.IsDeleted = 0
left join {{ source("abp_ref_models", "AbpTenants") }} abp2
on abp2.Id = e.DestinationTenantId

where abp.IsDeleted = 0 and abp.IsActive = 1
and r.IsDeleted = 0 and r.Status = 1
