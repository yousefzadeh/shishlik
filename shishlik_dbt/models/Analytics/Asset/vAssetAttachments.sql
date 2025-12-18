select
id.TenantId,
id.IssueId Asset_Id,
id.Id AssetAttachment_Id,
id.DisplayFileName AssetAttachment_Name,
id.CreationTime AssetAttachment_UploadTime,
id.CreatorUserId AssetAttachment_UserId,
au.Name+' '+au.Surname AssetAttachment_UserName

from {{ source("asset_ref_models", "IssueDocument") }} id
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = id.CreatorUserId and au.IsDeleted = 0 and au.IsActive = 1
where id.IsDeleted = 0