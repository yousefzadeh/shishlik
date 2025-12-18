select
ia.Uuid,
ia.TenantId,
ia.IssueId Asset_Id,
ia.Id AssetTask_Id,
ia.RegisterItemTaskId AssetTask_IdRef,
ia.Title AssetTask_Name,
ia.Description AssetTask_Description,
ia.Status AssetTask_StatusEnum,
case
when Status = 0 then 'New'
when Status = 3 then 'In Progress'
when Status = 1 then 'Completed'
end as AssetTask_Status,
ia.DueDate AssetTask_DueDate,
ia.UserId AssetTask_AssigneeId,
au.Name+' '+au.Surname AssetTask_AssigneeName,
ia.TenantVendorId AssetTask_TenantVendorId,
tv.Name AssetTask_ThirdParty,
ia.CompletedDate AssetTask_CompletedDate,
ia.JiraReferenceId AssetTask_JiraReferenceId,
ia.JiraUserName AssetTask_JiraUserName,
ia.JiraUserReferenceId AssetTask_JiraUserReferenceId

from {{ source("asset_ref_models", "IssueAction") }} ia
left join {{ source("third-party_ref_models", "TenantVendor") }} tv
on tv.Id = ia.TenantVendorId and tv.IsDeleted = 0 and tv.IsArchived = 0
left join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = ia.UserId and au.IsDeleted = 0 and au.IsActive = 1
where ia.IsDeleted = 0