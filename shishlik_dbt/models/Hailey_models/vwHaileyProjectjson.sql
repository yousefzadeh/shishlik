with base as(
    select
        p.TenantId tenant_id,
        p.Id id,
        p.CreationTime CreationDate,
        p.Name record_name,
        p.Description Description, 
        au.Name +' '+ au.Surname Owner

    from {{ source("hailey_models", "Project") }} p
    join {{ source("hailey_models", "AbpTenants") }} t
    on t.Id = p.TenantId
    left join {{ source("hailey_models", "AbpUsers") }} au
    on au.Id = p.OwnerId and au.IsDeleted = 0 and au.IsActive = 1
    where p.IsDeleted = 0
    and p.IsArchived = 0
    and p.IsTemplate = 0
    and t.IsDeleted = 0
    and t.IsActive = 1
)
, proj_tag as (
    select rtg.TenantId, rtg.ProjectId, STRING_AGG(t.Name, ', ') Tag
    from {{ source("hailey_models", "ProjectTag") }} rtg
    join {{ source("hailey_models", "Tags") }} t on rtg.TagId = t.Id and rtg.TenantId = t.TenantId and t.IsDeleted = 0
    where rtg.IsDeleted = 0
    group by rtg.ProjectId, rtg.TenantId
)

select
    p.tenant_id,
    p.id,
    p.record_name Name,
    p.Description,
    p.CreationDate,
    p.Owner,
    pt.Tag
from base p
left join proj_tag pt on pt.ProjectId = p.id