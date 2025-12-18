select
i.Uuid,
i.TenantId,
abp.Name TenantName,
i.Id Asset_Id,
i.EntityRegisterItemId Asset_IdRef,
i.Name Asset_Name,
i.Description Asset_Description,
i.ReportedBy Asset_IdentifiedBy,
i.RecordedDate Asset_ReportedDate,
i.DueDate Asset_DueDate,
i.Priority Asset_PriorityEnum,
case 
when i.Priority = 1 then '1 - Immediate'
when i.Priority = 2 then '2 - High'
when i.Priority = 3 then '3 - Medium'
when i.Priority = 4 then '4 - Low'
else 'Not selected' end as Asset_Priority,
i.WorkflowStageId Asset_StageId,
case when i.WorkflowStageId is null then 'Unassigned' else wfs.Name end Asset_Stage,
i.EntityRegisterId Asset_EntityRegisterId

from {{ source("asset_ref_models", "Issues") }} i
join {{ source("abp_ref_models", "AbpTenants") }} abp on abp.Id = i.TenantId
join {{ source("register_ref_models", "EntityRegister") }} er on er.Id = i.EntityRegisterId and er.IsDeleted = 0
left join {{ source("miscellaneous_ref_models", "WorkflowStage") }} wfs on wfs.Id = i.WorkflowStageId and wfs.IsDeleted = 0
left join {{ source("abp_ref_models", "AbpUsers") }} au on au.Id = i.PublishedById and au.IsDeleted = 0
where abp.IsDeleted = 0 and abp.IsActive = 1
and er.EntityType = 5
and i.IsDeleted = 0 and i.IsArchived = 0 and i.Status = 1