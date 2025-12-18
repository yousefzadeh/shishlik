with ctrl as (
select
c.Uuid,
c.TenantId,
c.PolicyDomainId PolicyDomain_Id,
c.Id Controls_Id,
c.Reference Controls_IdRef,
c.Name Controls_Name,
case
when lead([CreationTime]) over (partition by coalesce([RootControlId], [Id]) order by [CreationTime]) is null then 1
else 0 end Controls_IsCurrent

from {{ source("controlset_ref_models", "Controls") }} c
where c.IsDeleted = 0
)

select
* from ctrl c
where c.Controls_IsCurrent = 1
