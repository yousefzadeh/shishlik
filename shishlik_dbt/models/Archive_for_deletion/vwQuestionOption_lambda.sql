{{ config(materialized="view") -}}

{# 
with 
batch as (
	select *
	from {{ ref("QuestionOption_batch") }} 
)
,  
stream as (
	select * 
    from {{ ref("vwQuestionOption_source") }}
	where Question_UpdateTime > ( select max(batch.Question_UpdateTime) from batch )
)
select * from batch 
union all 
select * from stream
 #}
select *
from {{ ref("vwQuestionOption_source") }}
