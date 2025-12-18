select
id.TenantId,
id.IssueId Issue_Id,
id.Id IssueAttachment_Id,
id.DisplayFileName IssueAttachment_Name,
id.CreationTime IssueAttachment_UploadTime,
id.CreatorUserId IssueAttachment_UserId,
au.Name+' '+au.Surname IssueAttachment_UserName

from {{ source("issue_ref_models", "IssueDocument") }} id
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = id.CreatorUserId and au.IsDeleted = 0 and au.IsActive = 1
where id.IsDeleted = 0