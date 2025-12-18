select
ptc.TenantId,
ptc.Id ProjectTask_CommentId,
ptc.CreationTime ProjectTask_CommentCreationTime,
ptc.ProjectTaskId ProjectTask_Id,
ptc.CommentText ProjectTask_Comment,
ptc.UserId ProjectTask_CommentedUserId,
au.Name+' '+au.Surname ProjectTask_CommentedUserName

from {{ source("project_ref_models", "ProjectTaskComment") }} ptc
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = ptc.UserId and au.IsDeleted = 0 and au.IsActive = 1
where ptc.IsDeleted = 0