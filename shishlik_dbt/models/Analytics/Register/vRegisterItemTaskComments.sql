select
iac.TenantId,
iac.IssueActionId RegisterItemTask_Id,
iac.Id AssetTaskComment_Id,
iac.CreationTime RegisterItemTask_CommentCreationTime,
iac.Comment RegisterItemTask_Comment,
iac.UserId RegisterItemTask_CommentedUserId,
au.Name+' '+au.Surname RegisterItemTask_CommentedUserName

from {{ source("register_ref_models", "IssueActionComment") }} iac
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = iac.UserId and au.IsDeleted = 0 and au.IsActive = 1
where iac.IsDeleted = 0
and iac.IsUserActivity = 0