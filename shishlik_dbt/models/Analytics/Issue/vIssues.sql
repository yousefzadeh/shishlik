select
i.Uuid,
i.TenantId,
abp.Name TenantName,
i.Id Issues_Id,
i.EntityRegisterItemId Issues_IdRef,
i.IdRef Issues_LegacyIdRef,
i.Name Issues_Name,
i.Description Issues_Description,
i.ResolvedDate Issues_ResolvedDate,
i.PublishedById Issues_publishedById,
au.Name+' '+au.Surname Issues_PublishedBy,
i.ReportedBy Issues_IdentifiedBy,
i.RecordedDate Issues_ReportedDate,
i.DueDate Issues_DueDate,
i.Priority Issues_PriorityEnum,
case 
when i.Priority = 1 then '1 - Immediate'
when i.Priority = 2 then '2 - High'
when i.Priority = 3 then '3 - Medium'
when i.Priority = 4 then '4 - Low'
else 'Not selected' end as Issues_Priority,
i.IssueSubmissionFormId,
i.WorkflowStageId Issues_StageId,
wfs.Name Issues_Stage,
i.EntityRegisterId Issues_EntityRegisterId

from {{ source("issue_ref_models", "Issues") }} i
join {{ source("abp_ref_models", "AbpTenants") }} abp on abp.Id = i.TenantId
join {{ source("register_ref_models", "EntityRegister") }} er on er.Id = i.EntityRegisterId and er.IsDeleted = 0
left join {{ source("miscellaneous_ref_models", "WorkflowStage") }} wfs on wfs.Id = i.WorkflowStageId and wfs.IsDeleted = 0
left join {{ source("abp_ref_models", "AbpUsers") }} au on au.Id = i.PublishedById and au.IsDeleted = 0
where abp.IsDeleted = 0 and abp.IsActive = 1
and er.EntityType = 3
and i.IsDeleted = 0 and i.IsArchived = 0 and i.Status = 1