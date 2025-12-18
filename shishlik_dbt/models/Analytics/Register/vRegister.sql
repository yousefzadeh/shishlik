select
er.TenantId,
er.Id Register_Id,
er.CreationTime Register_CreationTime,
er.Name Register_Name,
er.Description Register_Description,
er.EntityType Register_EntityTypeCode,
er.IsWorkflowEnabled Register_IsWorkflowEnabledFlag,
er.Color Register_TextColor

from {{ source("register_ref_models", "EntityRegister") }} er
join {{ source("abp_ref_models", "AbpTenants") }} t
on t.Id = er.TenantId 
where er.IsDeleted = 0
and t.IsDeleted = 0 and t.IsActive = 1
and er.EntityType = 4