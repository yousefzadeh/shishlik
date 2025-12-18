select
ptd.TenantId,
ptd.Id ProjectTask_DocumentId,
ptd.ProjectTaskId ProjectTask_Id,
ptd.CreationTime ProjectTask_UploadTime,
ptd.DisplayName ProjectTask_DocumentName

from {{ source("project_ref_models", "ProjectTaskDocument") }} ptd
where ptd.IsDeleted = 0