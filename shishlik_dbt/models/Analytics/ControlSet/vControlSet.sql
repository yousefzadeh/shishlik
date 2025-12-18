with cs as (
select
p.Uuid,
p.TenantId,
p.Id ControlSet_Id,
p.Name ControlSet_Name,
case when lead(p.CreationTime) over (partition by coalesce(p.RootPolicyId, p.Id)  order by p.Version) is null then 1 else 0 end ControlSet_Iscurrent

from {{ source("controlset_ref_models", "Policy") }} p
where p.IsDeleted = 0
and p.Status != 100
)

select
*
from cs p