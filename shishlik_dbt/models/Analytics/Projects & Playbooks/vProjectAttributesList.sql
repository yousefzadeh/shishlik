with prj as (
select
p.TenantId,
p.Project_Id,
p.Project_Name
from {{ ref("vProject") }} p
)
, tag as (
select
pt.TenantId, pt.Project_Id, string_agg(pt.Project_Tag, '; ') Project_TagList
from {{ ref("vProjectTag") }} pt
group by
pt.TenantId, pt.Project_Id
)

select
p.TenantId,
p.Project_Id,
p.Project_Name,
t.Project_TagList

from prj p
left join tag t on t.Project_Id = p.Project_Id