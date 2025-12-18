{{ config(materialized="view") -}}
{# 
with 
batch as (
	select *
	from {{ ref("AnswerResponse_batch") }} 
)
,  
stream as (
	select * 
    from {{ ref("vwAnswerResponse_source") }}
	--where Answer_UpdateTime > ( select max(batch.Answer_UpdateTime) from batch )
)
select * from batch 
union all 
select * from stream
#}
select *
from {{ ref("vwAnswerResponse_source") }}
