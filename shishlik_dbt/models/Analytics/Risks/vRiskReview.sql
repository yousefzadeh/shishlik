select
rr.TenantId,
rr.Id RiskReview_Id,
rr.CreationTime RiskReview_CreationTime,
rr.Name RiskReview_Name,
rr.Description RiskReview_Description,
rr.Status RiskReview_StatusId,
case 
when rr.Status = 1 then 'New'
when rr.Status = 2 then 'In progress'
when rr.Status = 3 then 'Completed'
when rr.Status = 4 then 'Closed'
end RiskReview_Status,
rr.StartDate RiskReview_StartDate,
rrn.RiskId Risk_Id,
rrn.IsReviewed RiskReview_IsReviewFlag,
case
when rrn.IsReviewed = 0 then 'Not reviewed'
when rrn.IsReviewed = 1 then 'Reviewed'
end RiskReview_IsReviewed,
rrn.ReviewedDate RiskReview_ReviewedDate,
au.Name+' '+au.Surname RiskReview_ReviewedBy

from {{ source("risk_ref_models", "RiskReviewNew") }} rr
join {{ source("risk_ref_models", "ReviewRisksNew") }} rrn
on rrn.RiskReviewId = rr.Id and rrn.IsDeleted = 0
left join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = rrn.ReviewedById and au.IsDeleted = 0 and au.IsActive = 1
where rr.IsDeleted = 0
and rr.IsArchived = 0