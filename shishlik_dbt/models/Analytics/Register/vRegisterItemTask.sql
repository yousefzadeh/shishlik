select
ia.Uuid,
ia.TenantId,
ia.IssueId RegisterItem_Id,
ia.Id RegisterItemTask_Id,
ia.RegisterItemTaskId RegisterItemTask_IdRef,
ia.Title RegisterItemTask_Name,
ia.Description RegisterItemTask_Description,
ia.Status RegisterItemTask_StatusEnum,
case
when Status = 0 then 'New'
when Status = 3 then 'In Progress'
when Status = 1 then 'Completed'
end as RegisterItemTask_Status,
ia.DueDate RegisterItemTask_DueDate,
ia.UserId RegisterItemTask_AssigneeId,
au.Name+' '+au.Surname RegisterItemTask_AssigneeName,
ia.TenantVendorId RegisterItemTask_TenantVendorId,
tv.Name RegisterItemTask_ThirdParty,
ia.CompletedDate RegisterItemTask_CompletedDate,
ia.JiraReferenceId RegisterItemTask_JiraReferenceId,
ia.JiraUserName RegisterItemTask_JiraUserName,
ia.JiraUserReferenceId RegisterItemTask_JiraUserReferenceId

from {{ source("register_ref_models", "IssueAction") }} ia
left join {{ source("third-party_ref_models", "TenantVendor") }} tv
on tv.Id = ia.TenantVendorId and tv.IsDeleted = 0 and tv.IsArchived = 0
left join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = ia.UserId and au.IsDeleted = 0 and au.IsActive = 1
where ia.IsDeleted = 0