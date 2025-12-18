select
rc.Uuid,
rc.TenantId,
rc.Id Risk_CommentId,
rc.CreationTime Risk_CommentCreationTime,
rc.RiskId Risk_Id,
rc.Comment Risk_Comment,
rc.UserId Risk_CommentedUserId,
au.Name+' '+au.Surname Risk_CommentedUserName

from {{ source("risk_ref_models", "RiskComment") }} rc
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = rc.UserId and au.IsDeleted = 0 and au.IsActive = 1
where rc.IsDeleted = 0