with base as(
select 
iss.TenantId tenant_id,
iss.Id id,
iss.Name record_name,
iss.Description Description,
iss.CreationTime CreationDate

from {{ source("hailey_models", "Issues") }} iss
    join {{ source("hailey_models", "EntityRegister") }} er
    on er.Id = iss.EntityRegisterId 
    and er.IsDeleted = 0 and er.EntityType = 4  
where iss.IsDeleted = 0
)
, rr_tag as (
select it.TenantId, it.IssueId, STRING_AGG(t.Name, ', ') Tag
from {{ source("hailey_models", "IssueTag") }} it
    join {{ source("hailey_models", "Tags") }} t 
    on it.TagId = t.Id 
    and it.TenantId = t.TenantId and t.IsDeleted = 0
where it.IsDeleted = 0
group by it.TenantId, it.IssueId
)
, rr_risk as (
select irr.TenantId, irr.IssueId, STRING_AGG(r.Name, ', ') LinkedRisk
from {{ source("hailey_models", "IssueRisk") }} irr
    join {{ source("hailey_models", "Risk") }} r
    on r.Id = irr.RiskId and r.IsDeleted = 0
where irr.IsDeleted = 0
group by irr.TenantId, irr.IssueId
)
, rr_owner_base as (
select io.TenantId, io.IssueId, au.Name+' '+au.Surname Owner
from {{ source("hailey_models", "IssueOwner") }} io
    left join {{ source("hailey_models", "AbpUsers") }} au 
    on io.UserId = au.Id and au.IsDeleted = 0
where io.IsDeleted = 0

union all

select io.TenantId, io.IssueId, aou. DisplayName Owner
from {{ source("hailey_models", "IssueOwner") }} io
    left join {{ source("hailey_models", "AbpOrganizationUnits") }} aou 
    on io.OrganizationUnitId = aou.Id and aou.IsDeleted = 0
where io.IsDeleted = 0
)
, rr_owner as (
select ro.TenantId, ro.IssueId, STRING_AGG(ro.owner, ', ') Owner
from rr_owner_base ro
group by ro.TenantId, ro.IssueId
)

select
rr.tenant_id,
rr.id,
rr.record_name Name,
rr.Description,
rr.CreationDate,
rrt.Tag,
rrr.LinkedRisk,
ro.Owner

from base rr
left join rr_tag rrt on rrt.IssueId = rr.id
left join rr_risk rrr on rrr.IssueId = rr.id
left join rr_owner ro on ro.IssueId = rr.id