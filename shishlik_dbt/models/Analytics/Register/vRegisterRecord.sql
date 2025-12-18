select
i.Uuid,
i.TenantId,
abp.Name TenantName,
er.Id Register_Id,
er.Name Register_Name,
i.Id Record_Id,
i.EntityRegisterItemId RegisterRecord_IdRef,
i.Name Record_Name,
i.Description RegisterRecord_Description,
i.ReportedBy RegisterRecord_IdentifiedBy,
i.RecordedDate RegisterRecord_ReportedDate,
i.DueDate RegisterRecord_DueDate,
i.Priority RegisterRecord_PriorityEnum,
case 
when i.Priority = 1 then '1 - Immediate'
when i.Priority = 2 then '2 - High'
when i.Priority = 3 then '3 - Medium'
when i.Priority = 4 then '4 - Low'
else 'Not selected' end as RegisterRecord_Priority,
i.WorkflowStageId RegisterRecord_StageId,
case when i.WorkflowStageId is null then 'Unassigned' else wfs.Name end RegisterRecord_Stage,
i.EntityRegisterId RegisterRecord_EntityRegisterId

from {{ source("register_ref_models", "Issues") }} i
join {{ source("abp_ref_models", "AbpTenants") }} abp on abp.Id = i.TenantId
join {{ source("register_ref_models", "EntityRegister") }} er on er.Id = i.EntityRegisterId and er.IsDeleted = 0
left join {{ source("miscellaneous_ref_models", "WorkflowStage") }} wfs on wfs.Id = i.WorkflowStageId and wfs.IsDeleted = 0
left join {{ source("abp_ref_models", "AbpUsers") }} au on au.Id = i.PublishedById and au.IsDeleted = 0
where abp.IsDeleted = 0 and abp.IsActive = 1
and er.EntityType = 4
and i.IsDeleted = 0 and i.IsArchived = 0 and i.Status = 1