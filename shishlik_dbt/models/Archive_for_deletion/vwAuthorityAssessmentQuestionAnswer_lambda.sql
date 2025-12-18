{{ config(materialized="view") -}}
{#
with 
batch as (
	select *
	from {{ ref("AuthorityAssessmentQuestionAnswer_batch") }} 
)
, stream as (
	select * 
    from {{ ref("vwAuthorityAssessmentQuestionAnswer_source") }}
	where AssessmentQuestionAnswer_UpdateTime > ( select max(batch.AssessmentQuestionAnswer_UpdateTime) from batch )
)
select * from batch 
union all 
select * from stream
#}
select *
from {{ ref("vwAuthorityAssessmentQuestionAnswer_source") }}
