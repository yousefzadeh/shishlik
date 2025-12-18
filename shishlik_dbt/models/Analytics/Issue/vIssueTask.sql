select
ia.Uuid,
ia.TenantId,
ia.IssueId Issue_Id,
ia.Id IssueTask_Id,
ia.IdRef IssueTask_LegacyIdRef,
ia.RegisterItemTaskId IssueTask_IdRef,
ia.Title IssueTask_Name,
ia.Description IssueTask_Description,
ia.Status IssueTask_StatusEnum,
case
when Status = 0 then 'New'
when Status = 3 then 'In Progress'
when Status = 1 then 'Completed'
end as IssueTask_Status,
ia.CreationTime IssueTask_CreationTime,
ia.LastModificationTime IssueTask_LastModificationTime,
ia.DueDate IssueTask_DueDate,
ia.UserId IssueTask_AssigneeId,
au.Name+' '+au.Surname IssueTask_AssigneeName,
ia.TenantVendorId IssueTask_TenantVendorId,
tv.Name IssueTask_ThirdParty,
ia.CompletedDate IssueTask_CompletedDate,
ia.JiraReferenceId IssueTask_JiraReferenceId,
ia.JiraUserName IssueTask_JiraUserName,
ia.JiraUserReferenceId IssueTask_JiraUserReferenceId

from {{ source("issue_ref_models", "IssueAction") }} ia
left join {{ source("third-party_ref_models", "TenantVendor") }} tv
on tv.Id = ia.TenantVendorId and tv.IsDeleted = 0 and tv.IsArchived = 0
left join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = ia.UserId and au.IsDeleted = 0 and au.IsActive = 1
where ia.IsDeleted = 0