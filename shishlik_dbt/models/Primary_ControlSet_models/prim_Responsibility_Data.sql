with resp as(
select
p.TenantId,
p.Id Policy_Id,
p.[Name] Policy_Name,
s.Id Responsibility_Id,
s.IsDeleted,
s.Title Responsibility_Name,
s.[Description] Responsibility_Description,
s.DueDate Responsibility_DueDate,
case
when [ActionStatus] = 1 then 'No'
when [ActionStatus] != 1 and s.DueDate > getdate() then 'No'
when [ActionStatus] != 1 and s.DueDate < getdate() then 'Yes'
else 'No'
end Responsibility_IsOverDue,
case when HasPeriod = 1 then 'Yes' else 'No' end Responsibility_HasPeriod,
case
when [Period] = 1
then 'Every Year'
when [Period] = 2
then 'Every 6 Months'
when [Period] = 3
then 'Every Month'
when [Period] = 4
then 'Every Week'
when [Period] = 6
then 'Every 3 Months'
when [Period] = 7
then 'Every Day'
else 'Once-off'
end Responsibility_TimePeriod,
s.PeriodStartDate Responsibility_PeriodStartDate,
case
when [ActionStatus] = 0 then 'New' when [ActionStatus] = 1 then 'Completed' else 'Undefined'
end as Responsibility_ActionStatus,
sc.Comment Responsibility_Comment,
roa.ownernamelist Responsibility_OwnerList,
roa.assigneenamelist Responsibility_AssigneeList,
case when lead(s.[CreationTime]) over (partition by coalesce([RootStatementId],s.[Id])  order by s.[Version]) is null then 1 else 0 end  IsCurrent

from {{ source("statement_models", "Statement") }} s
left join {{ source("statement_models", "StatementPolicy") }} sp
on s.Id = sp.StatementId and s.TenantId = sp.TenantId and s.IsDeleted = 0 and sp.IsDeleted = 0
left join {{ source("assessment_models", "Policy") }} p
on sp.PolicyId = p.Id and sp.TenantId = p.TenantId and sp.IsDeleted = 0 AND p.HideResponsibilityTasksUntilRepublished = 0
left join {{ source("statement_models", "StatementComment") }} sc on sc.StatementId = s.Id and sc.IsDeleted = 0
left join {{ ref("vwResponsibilityOwnerAssigneeList") }} roa on roa.responsibility_id	= s.Id
where p.IsDeleted = 0
and p.[Status] != 100
)
, resp_ctrl as (
select
r.TenantId,
r.Responsibility_Id,
STRING_AGG(CONVERT(NVARCHAR(max), c.[Name]), ', ') Responsibility_LinkedControlsList
from resp r
left join {{ source("statement_models", "StatementControl") }} sc
on sc.StatementId = r.Responsibility_Id and sc.TenantId = r.TenantId and sc.IsDeleted = 0
left join {{ source("assessment_models", "Controls") }} c
on c.Id = sc.ControlId and c.TenantId = sc.TenantId and c.IsDeleted = 0
group by
r.TenantId,
r.Responsibility_Id
)

select 
r.TenantId,
r.Policy_Id,
r.IsDeleted,
r.Policy_Name,
r.Responsibility_Id,
r.Responsibility_Name,
r.Responsibility_Description,
r.Responsibility_DueDate,
r.Responsibility_IsOverDue,
r.Responsibility_HasPeriod,
r.Responsibility_TimePeriod,
r.Responsibility_PeriodStartDate,
r.Responsibility_ActionStatus,
r.Responsibility_Comment,
r.Responsibility_OwnerList,
r.Responsibility_AssigneeList,
rc.Responsibility_LinkedControlsList
from resp r
left join resp_ctrl rc
on rc.TenantId = r.TenantId
and rc.Responsibility_Id = r.Responsibility_Id
where r.IsCurrent = 1