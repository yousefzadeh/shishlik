with prj_tmp as (
select
pt.TenantId,
pt.ProjectTemplate_Id,
pt.ProjectTemplate_Name
from {{ ref("vProjectTemplate") }} pt
)
, tag as (
select
pt.TenantId, pt.Project_Id, string_agg(pt.Project_Tag, '; ') ProjectTemplate_TagList
from {{ ref("vProjectTag") }} pt
group by
pt.TenantId, pt.Project_Id
)

select
pt.TenantId,
pt.ProjectTemplate_Id,
pt.ProjectTemplate_Name,
t.ProjectTemplate_TagList

from prj_tmp pt
left join tag t on t.Project_Id = pt.ProjectTemplate_Id