with base as(
select 
*,
case when lead(p.CreationTime) over (partition by coalesce(p.RootPolicyId,p.Id)  order by [Version]) is null then 1 else 0 end  IsCurrent

from {{ source("hailey_models", "Policy") }} p
where p.IsDeleted = 0
and p.Status != 100
)
, cs as (
select
    b.TenantId tenant_id,
    b.Id id,
    b.Name record_name,
    b.Description AS Description,
    b.CreationTime CreationDate,
    b.NextReviewDate,
    b.LastReviewDate
from base b
where b.IsCurrent = 1
)
, cs_owner as (
select ps.TenantId, ps.PolicyId, STRING_AGG(ps.StakeHolderName, ', ') Owner
from {{ source("hailey_models", "PolicyStakeHolders") }} ps
where ps.IsDeleted = 0 and ps.Role = 1
group by ps.TenantId, ps.PolicyId
)
, cs_rvwr as (
select ps.TenantId, ps.PolicyId, STRING_AGG(ps.StakeHolderName, ', ') Reviewer
from {{ source("hailey_models", "PolicyStakeHolders") }} ps
where ps.IsDeleted = 0 and ps.Role = 2
group by ps.TenantId, ps.PolicyId
)
, cs_rd as (
select ps.TenantId, ps.PolicyId, STRING_AGG(ps.StakeHolderName, ', ') Reader
from {{ source("hailey_models", "PolicyStakeHolders") }} ps
where ps.IsDeleted = 0 and ps.Role = 3
group by ps.TenantId, ps.PolicyId
)
, cs_app as (
select ps.TenantId, ps.PolicyId, STRING_AGG(ps.StakeHolderName, ', ') Approver
from {{ source("hailey_models", "PolicyStakeHolders") }} ps
where ps.IsDeleted = 0 and ps.Role = 4
group by ps.TenantId, ps.PolicyId
)


select
p.tenant_id,
p.id,
p.record_name as Name,
p.Description,
p.CreationDate,
p.NextReviewDate,
p.LastReviewDate,
co.Owner,
cr.Reviewer,
crd.Reader,
ca.Approver

from cs p
left join cs_owner co on co.PolicyId = p.id
left join cs_rvwr cr on cr.PolicyId = p.id
left join cs_rd crd on crd.PolicyId = p.id
left join cs_app ca on ca.PolicyId = p.id