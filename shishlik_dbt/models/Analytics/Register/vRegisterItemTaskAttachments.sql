select
iad.IssueActionId RegisterItemTask_Id,
iad.Id RegisterItemTaskAttachment_Id,
iad.DisplayFileName RegisterItemTaskAttachment_Name,
iad.CreationTime RegisterItemTaskAttachment_UploadTime,
iad.CreatorUserId RegisterItemTaskAttachment_UserId,
au.Name+' '+au.Surname RegisterItemTaskAttachment_UserName

from {{ source("register_ref_models", "IssueActionDocument") }} iad
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = iad.CreatorUserId and au.IsDeleted = 0 and au.IsActive = 1
where iad.IsDeleted = 0