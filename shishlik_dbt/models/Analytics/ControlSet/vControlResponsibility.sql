select
s.Uuid,
s.TenantId,
s.Id Responsibility_Id,
s.CreationTime Responsibility_CreationTime,
s.Title Responsibility_Name,
s.[Description] Responsibility_Description,
s.DueDate Responsibility_DueDate,
s.[Status] Responsibility_StatusCode,
case
when s.[Status] = 1 then 'Edit'
when s.[Status] = 2 then 'Published'
when s.[Status] = 100 then 'Deprecated'
else 'Undefined' end Responsibility_Status,
s.PublishedDate Responsibility_PublishedDate,
s.HasPeriod Responsibility_HasPeriod,
s.[Period] Responsibility_PeriodCode,
case
when [Period] = 1 then 'Every Year'
when [Period] = 2 then 'Every 6 Months'
when [Period] = 3 then 'Every Month'
when [Period] = 4 then 'Every Week'
when [Period] = 6 then 'Every 3 Months'
when [Period] = 7 then 'Every Day'
else 'Once-off' end Responsibility_Period,
s.PeriodicStatementId Responsibility_PeriodicStatementId,
s.PeriodStartDate Responsibility_PeriodStartDate,
s.TemplateStatementId Responsibility_TemplateStatementId,
s.[Version] Responsibility_Version,
s.ParentStatementId Responsibility_ParentStatementId,
s.RootStatementId Responsibility_RootStatementId,
s.[Order] Responsibility_Order,
s.ActionStatus Responsibility_ActionStatusCode,
case
when [ActionStatus] = 0 then 'New'
when [ActionStatus] = 1 then 'Completed'
else 'Undefined' end as Responsibility_ActionStatus

from {{ source("controlset_ref_models", "Statement") }} s
where s.IsDeleted = 0
and s.Status != 100