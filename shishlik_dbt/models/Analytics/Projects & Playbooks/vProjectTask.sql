select
pt.TenantId,
pt.ProjectId Project_Id,
pt.Id ProjectTask_Id,
pt.Ordinal ProjectTask_Ordinal,
pt.Name ProjectTask_Name,
pt.Description ProjectTask_Description,
pt.CreationTime ProjectTask_CreationTime,
pt.LastModificationTime ProjectTask_LastModificationTime,
pt.DueDate ProjectTask_DueDate,
pt.Status ProjectTask_StatusCode,
case
when pt.Status = 0 then 'New'
when pt.Status = 1 then 'Completed'
when pt.Status = 3 then 'In Progress'
when pt.Status = 2 then 'Closed'
end ProjectTask_Status

from {{ source("project_ref_models", "ProjectTask") }} pt
where pt.IsDeleted = 0
and pt.IsArchived = 0
and pt.ParentTaskId is null