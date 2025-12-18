with base as(
select
p.id bid,
c.TenantId tenant_id,
c.Id id,
c.CreationTime CreationDate,
c.Name record_name,
pd.Name PolicyDomainName,
c.Detail as Description,
c.Reference,
p.Name as PolicyName
{# case
when lead(c.[CreationTime]) over (partition by coalesce(c.[RootControlId], c.[Id]) order by c.[CreationTime]) is null then 1
else 0 end IsCurrent #}

from {{ source("hailey_models", "Policy") }} p
join {{ source("hailey_models", "AbpTenants") }} t
on t.Id = p.TenantId
join {{ source("hailey_models", "PolicyDomain") }} pd
on pd.PolicyId = p.Id and pd.IsDeleted = 0
join {{ source("hailey_models", "Controls") }} c
on c.PolicyDomainId = pd.Id and c.IsDeleted = 0
where p.IsDeleted = 0
and p.Status != 100
and t.IsDeleted = 0
and t.IsActive = 1
)
, ctrl_owner_base as (
select co.TenantId, co.ControlId, au.Name+' '+au.Surname owner
from {{ source("hailey_models", "ControlOwner") }} co
left join {{ source("hailey_models", "AbpUsers") }} au on co.UserId = au.Id and au.IsDeleted = 0
where co.IsDeleted = 0

union all

select co.TenantId, co.ControlId, aou. DisplayName owner
from {{ source("hailey_models", "ControlOwner") }} co
left join {{ source("hailey_models", "AbpOrganizationUnits") }} aou on co.OrganizationUnitId = aou.Id and aou.IsDeleted = 0
where co.IsDeleted = 0
)
, ctrl_owner as (
select co.TenantId, co.ControlId, STRING_AGG(co.owner, ', ') Owner
from ctrl_owner_base co
group by co.TenantId, co.ControlId
)

select
p.tenant_id,
p.id,
p.record_name Name,
p.CreationDate,
p.PolicyDomainName,
co.Owner,
p.Description,
p.Reference,
p.PolicyName

from base p
left join ctrl_owner co
on co.ControlId = p.id
{# where p.IsCurrent = 1 #}