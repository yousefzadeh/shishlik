select
iac.TenantId,
iac.IssueActionId AssetTask_Id,
iac.Id AssetTaskComment_Id,
iac.CreationTime AssetTask_CommentCreationTime,
iac.Comment AssetTask_Comment,
iac.UserId AssetTask_CommentedUserId,
au.Name+' '+au.Surname AssetTask_CommentedUserName

from {{ source("asset_ref_models", "IssueActionComment") }} iac
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = iac.UserId and au.IsDeleted = 0 and au.IsActive = 1
where iac.IsDeleted = 0
and iac.IsUserActivity = 0