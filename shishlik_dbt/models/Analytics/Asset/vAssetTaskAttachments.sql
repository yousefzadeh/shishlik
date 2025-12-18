select
iad.IssueActionId AssetTask_Id,
iad.Id AssetTaskAttachment_Id,
iad.DisplayFileName AssetTaskAttachment_Name,
iad.CreationTime AssetTaskAttachment_UploadTime,
iad.CreatorUserId AssetTaskAttachment_UserId,
au.Name+' '+au.Surname AssetTaskAttachment_UserName

from {{ source("asset_ref_models", "IssueActionDocument") }} iad
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = iad.CreatorUserId and au.IsDeleted = 0 and au.IsActive = 1
where iad.IsDeleted = 0