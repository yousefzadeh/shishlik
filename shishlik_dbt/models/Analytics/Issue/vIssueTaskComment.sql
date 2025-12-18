select
iac.TenantId,
iac.IssueActionId IssueTask_Id,
iac.Id IssueTaskComment_Id,
iac.CreationTime IssueTask_CommentCreationTime,
iac.Comment IssueTask_Comment,
iac.UserId IssueTask_CommentedUserId,
au.Name+' '+au.Surname IssueTask_CommentedUserName

from {{ source("issue_ref_models", "IssueActionComment") }} iac
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = iac.UserId and au.IsDeleted = 0 and au.IsActive = 1
where iac.IsDeleted = 0
and iac.IsUserActivity = 0