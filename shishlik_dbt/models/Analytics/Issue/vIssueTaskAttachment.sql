select
iad.IssueActionId IssueTask_Id,
iad.Id IssueTaskAttachment_Id,
iad.DisplayFileName IssueTaskAttachment_Name,
iad.CreationTime IssueTaskAttachment_UploadTime,
iad.CreatorUserId IssueTaskAttachment_UserId,
au.Name+' '+au.Surname IssueTaskAttachment_UserName

from {{ source("issue_ref_models", "IssueActionDocument") }} iad
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = iad.CreatorUserId and au.IsDeleted = 0 and au.IsActive = 1
where iad.IsDeleted = 0