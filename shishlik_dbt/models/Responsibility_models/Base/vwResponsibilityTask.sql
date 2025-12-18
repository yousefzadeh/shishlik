with Responsibility as (
select distinct
p.Policy_Id,
'Control Responsibility' [Type],
iif(S.Statement_RootStatementId is null, S.Statement_Id, S.Statement_RootStatementId) as RootStatementId,
sr.StatementResponse_Id ActionId,
s.Statement_Id,
s.Statement_Version [Version],
sr.StatementResponse_UserId UserId,
s.Statement_TenantId TenantId,
s.Statement_Title Title,
s.Statement_Description Description,
sr.StatementResponse_StatementDueDate DueDate,
case when sr.StatementResponse_Status = 1 then sr.StatementResponse_CompletedDate end CompletedDate,
case
when sr.StatementResponse_Status in (1, 2, 4)
then 'Not Overdue'
when sr.StatementResponse_Status in (0, 3) and getdate() <= sr.StatementResponse_StatementDueDate
then 'Not Overdue'
when getdate() > sr.StatementResponse_StatementDueDate
then 'Overdue'
else 'No Due Date'
end as DueDateStatus,
case
when sr.StatementResponse_Status = 1
then 'Completed'
when sr.StatementResponse_Status = 0
then 'New'
when sr.StatementResponse_Status = 3
then 'In-Progress'
else 'Undefined'
end [Status],
coalesce(sr.StatementResponse_LastModificationTime, sr.StatementResponse_CreationTime) as LastModificationDate,
au.AbpUsers_FullName AssigneeFilter

FROM {{ ref("vwStatementResponse") }} SR
	INNER JOIN {{ ref("vwStatement") }} S ON SR.StatementResponse_StatementId = S.Statement_Id 
	INNER JOIN {{ ref("vwStatementPolicy") }} SP ON SP.StatementPolicy_StatementId = S.Statement_Id 
	INNER JOIN {{ ref("vwPolicy") }} p ON SP.StatementPolicy_PolicyId = P.Policy_Id and p.Policy_Status != 1 AND p.Policy_HideResponsibilityTasksUntilRepublished = 0
    left join {{ ref("vwAbpUser") }} au on au.AbpUsers_Id = sr.StatementResponse_UserId
WHERE
S.Statement_Status != 1 AND SR.StatementResponse_IsDeprecated = 0
)
, max as(
select distinct 
    r. [Type],
    r.ActionId,
    r.TenantId,
    r.Policy_Id,
    r.Statement_Id,
    r.Title,
    r.Description,
    r.DueDate,
    r.CompletedDate,

    r.DueDateStatus,
    r.Status,
	r.UserId,
    r.AssigneeFilter,
	r.LastModificationDate,
    r.RootStatementId,
    r.[version],
    count(r.RootStatementId) over (PARTITION by r.TenantId, r.RootStatementId, r.UserId, r.DueDate ) as no_count,
    max(r.[Version]) over (PARTITION by r.RootStatementId) as max_version
from Responsibility r
)
, final as(
select max.*, max(max.no_count) over (PARTITION by max.RootStatementId) as max_no_count
from max
)

select 
    [Type],
    ActionId ResponsibilityTask_Id,
    TenantId,
    Policy_Id,
    Statement_Id ResponsibilityTask_ResponsibilityId,
    Title,
    [Description],
    DueDate ResponsibilityTask_DueDate,
    CompletedDate ResponsibilityTask_CompletedDate,

    DueDateStatus ResponsibilityTask_DueDateStatus,
    [Status] ResponsibilityTask_StatusCode,
	LastModificationDate ResponsibilityTask_LastModificationTime,
	UserId ResponsibilityTask_UserId,
    AssigneeFilter
from final f
where (f.max_no_count = 1  OR (f.max_no_count > 1 and f.max_version = f.[Version]))