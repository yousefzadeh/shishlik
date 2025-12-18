with ques as (
select
q.TenantId,
q.Question_Id,
q.Question_IdRef,
q.Question_Name
from {{ ref("vQuestion") }} q
)
, tags as (
select
qt.TenantId, qt.Question_Id, string_agg(qt.Question_Tag, '; ') Question_TagList
from {{ ref("vQuestionTag") }} qt
group by
qt.TenantId, qt.Question_Id
)

select
q.TenantId,
q.Question_Id,
q.Question_IdRef,
q.Question_Name,
t.Question_TagList

from ques q
left join tags t on t.TenantId = q.TenantId and t.Question_Id = q.Question_Id