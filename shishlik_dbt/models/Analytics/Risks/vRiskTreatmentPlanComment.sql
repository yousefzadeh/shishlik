select
rtpc.Uuid,
rtpc.TenantId,
rtpc.Id RiskTreatmentPlanComment_Id,
rtpc.RiskTreatmentPlanId RiskTreatmentPlan_Id,
rtpc.Comment RiskTreatmentPlan_Comment,
rtpc.UserId RiskTreatmentPlan_CommentedUserId,
au.Name+' '+au.Surname RiskTreatmentPlan_CommentedUserName

from {{ source("risk_ref_models", "RiskTreatmentPlanComment") }} rtpc
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = rtpc.UserId
where rtpc.IsDeleted = 0