with task_status as(
select 
aec.TenantId,
aec.EntityId ProjectTask_Id,
aec.ChangeTime,
aec.EntityTypeFullName,
aepc.OriginalValue ProjectTask_OldStatus,
aepc.NewValue ProjectTask_NewStatus,
row_number() OVER (PARTITION BY aec.TenantId, aec.EntityId ORDER BY aec.ChangeTime DESC) as ProjectTask_Status_LatestFlag,
datediff(day, aec.ChangeTime, getdate()) ProjectTask_Status_ElapsedDays
--case when aepc.OriginalValue is NULL then 0 else DATEDIFF(day, lag(aec.ChangeTime) over (order by aec.ChangeTime), aec.ChangeTime) end as Difference,


from {{ source("abp_ref_models", "AbpEntityChanges") }} aec
join {{ source("abp_ref_models", "AbpEntityPropertyChanges") }} aepc
on aepc.EntityChangeId = aec.Id
where aec.EntityTypeFullName = 'LegalRegTech.Projects.ProjectTask'
and aepc.PropertyNameVarChar = 'Status'
)

select
TenantId,
ProjectTask_Id,
ProjectTask_OldStatus,
ProjectTask_NewStatus,
ProjectTask_Status_ElapsedDays

from task_status ts
where ProjectTask_Status_LatestFlag = 1