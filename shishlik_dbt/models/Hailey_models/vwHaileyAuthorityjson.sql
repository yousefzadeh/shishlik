with base as (
    select
        a.TenantId tenant_id,
        a.Id id,
        a.CreationTime CreationDate,
        a.Type ,
        j.Name Jurisdiction,
        t.Name Owner,
        a.Name record_name,
        a.Description Description,
        a.Body AuthorityBody,
        a.AuthoritySector, 
        a.Url as AuthorityReferenceURL 
    from {{ source("hailey_models", "Authority") }} a
    join {{ source("hailey_models", "AbpTenants") }} t
    on t.Id = a.TenantId
    left join {{ source("hailey_models", "Jurisdiction") }} j
    on j.Id = a.JurisdictionId and j.IsDeleted = 0
    where a.IsDeleted = 0
    and a.IsArchived = 0
    and t.IsDeleted = 0
    and t.IsActive = 1

    union all

    select
        ta.TenantId tenant_id,
        ta.AuthorityId id,
        ta.CreationTime CreationDate,
        a.Type,
        j.Name Jurisdiction,
        '6clicks' Owner,
        a.Name record_name,
        a.Description Description,
        a.Body AuthorityBody,
        a.AuthoritySector, 
        a.Url as AuthorityReferenceURL 
    from {{ source("hailey_models", "TenantAuthority") }} ta
    join {{ source("hailey_models", "Authority") }} a
    on ta.AuthorityId = a.Id
    join {{ source("hailey_models", "AbpTenants") }} t
    on t.Id = ta.TenantId
    left join {{ source("hailey_models", "Jurisdiction") }} j
    on j.Id = a.JurisdictionId and j.IsDeleted = 0
    where ta.IsDeleted = 0
    and ta.IsArchived = 0
    and t.IsDeleted = 0
    and t.IsActive = 1
)

select
    a.tenant_id,
    a.id,
    a.record_name as Name,
    a.Description,
    a.CreationDate,
    a.Type ,
    a.Jurisdiction,
    a.Owner,
    a.AuthorityBody,
    a.AuthoritySector,
    a.AuthorityReferenceURL
from base a 