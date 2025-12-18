select
pt.TenantId,
pt.ProjectId Project_Id,
pt.ParentTaskId ProjectTask_Id,
pt.Id ProjectSubTask_Id,
pt.Ordinal ProjectSubTask_Ordinal,
pt.Name ProjectSubTask_Name,
pt.Description ProjectSubTask_Description,
pt.CreationTime ProjectSubTask_CreationTime,
pt.DueDate ProjectSubTask_DueDate,
pt.Status ProjectSubTask_StatusCode,
case
when pt.Status = 0 then 'New'
when pt.Status = 1 then 'Completed'
when pt.Status = 2 then 'Closed'
when pt.Status = 3 then 'In Progress'
end ProjectSubTask_Status

from {{ source("project_ref_models", "ProjectTask") }} pt
where pt.IsDeleted = 0
and pt.IsArchived = 0
and pt.ParentTaskId is not null