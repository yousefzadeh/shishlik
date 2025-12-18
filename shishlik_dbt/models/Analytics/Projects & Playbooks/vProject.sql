select
p.Uuid,
p.TenantId,
t.Name TenantName,
p.CreatedFromId ProjectTemplate_Id,
case when pt.Name is NULL then 'No Template' else pt.Name end ProjectTemplate_Name,
p.Id Project_Id,
p.Name Project_Name,
p.Description Project_Description,
p.CreationTime Project_CreationTime,
p.CreatorUserId Project_CreatorUserId,
p.LastModificationTime Project_LastModificationTime,
p.LastModifierUserId Project_LastModifierUserId,
p.DueDate Project_DueDate,
p.OwnerId Project_OwnerId,
au.Name +' '+ au.Surname Project_Owner,     
p.Status Project_StatusFlag,
case
when p.Status = 0 then 'Open'
when p.Status = 1 then 'Closed'
end Project_Status

from {{ source("project_ref_models", "Project") }} p
left join {{ source("project_ref_models", "Project") }} pt on pt.Id = p.CreatedFromId and pt.IsTemplate = 1
join {{ source("abp_ref_models", "AbpTenants") }} t on t.Id = p.TenantId
join {{ source("abp_ref_models", "AbpUsers") }} au on au.Id = p.OwnerId and au.IsDeleted = 0 and au.IsActive = 1
where p.IsDeleted = 0
and p.IsArchived = 0 and p.IsTemplate = 0
and t.IsDeleted = 0 and t.IsActive = 1