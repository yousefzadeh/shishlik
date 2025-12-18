select
p.Uuid,
p.TenantId,
t.Name TenantName,
p.Id ProjectTemplate_Id,
p.Name ProjectTemplate_Name,
p.Description ProjectTemplate_Description,
p.CreationTime ProjectTemplate_CreationTime,
p.CreatorUserId ProjectTemplate_CreatorUserId,
p.LastModificationTime ProjectTemplate_LastModificationTime,
p.LastModifierUserId ProjectTemplate_LastModifierUserId,
p.TemplateStatus ProjectTemplate_StatusCode,
case
when p.TemplateStatus = 1 then 'Draft'
when p.TemplateStatus = 2 then 'Published'
end ProjectTemplate_Status,
p.TemplateType ProjectTemplate_TypeCode,
case
when p.TemplateType = 0 then 'Draft'
when p.TemplateType = 1 then 'Internal'
when p.TemplateType = 2 then 'Content Library Item'
end ProjectTemplate_Type,
p.PublishedDate ProjectTemplate_PublishedDate,
p.PublishedById ProjectTemplate_PublishedById,
au.Name + ' '+ au.Surname ProjectTemplate_PublishedBy

from {{ source("project_ref_models", "Project") }} p
join {{ source("abp_ref_models", "AbpTenants") }} t on t.Id = p.TenantId
left join {{ source("abp_ref_models", "AbpUsers") }} au on au.Id = p.PublishedById and au.IsDeleted = 0 and au.IsActive = 1
where p.IsDeleted = 0
and p.IsArchived = 0 and p.IsTemplate = 1
and t.IsDeleted = 0 and t.IsActive = 1