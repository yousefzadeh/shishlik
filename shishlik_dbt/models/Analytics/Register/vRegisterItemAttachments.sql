select
id.TenantId,
id.IssueId RegisterItem_Id,
id.Id RegisterItemAttachment_Id,
id.DisplayFileName RegisterItemAttachment_Name,
id.CreationTime RegisterItemAttachment_UploadTime,
id.CreatorUserId RegisterItemAttachment_UserId,
au.Name+' '+au.Surname RegisterItemAttachment_UserName

from {{ source("register_ref_models", "IssueDocument") }} id
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = id.CreatorUserId and au.IsDeleted = 0 and au.IsActive = 1
where id.IsDeleted = 0