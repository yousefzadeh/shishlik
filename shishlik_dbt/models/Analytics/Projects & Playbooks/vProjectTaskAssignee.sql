with assignee as (
select
pta.TenantId,
pta.ProjectTaskId ProjectTask_Id,
pta.UserId AssigneeId,
au.Name+' '+au.Surname+' ('+au.EmailAddress+')' ProjectTask_Assignee
from {{ source("project_ref_models", "ProjectTaskAssignee") }} pta
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = pta.UserId and au.IsDeleted = 0 and au.IsActive = 1
where pta.IsDeleted = 0

union all

select
pta.TenantId,
pta.ProjectTaskId ProjectTask_Id,
pta.OrganizationUnitId AssigneeId,
aou.DisplayName ProjectTask_Assignee
from {{ source("project_ref_models", "ProjectTaskAssignee") }} pta
join {{ source("abp_ref_models", "AbpOrganizationUnits") }} aou
on aou.Id = pta.OrganizationUnitId and aou.IsDeleted = 0
where pta.IsDeleted = 0
)

select
TenantId,
ProjectTask_Id,
AssigneeId,
ProjectTask_Assignee
from assignee