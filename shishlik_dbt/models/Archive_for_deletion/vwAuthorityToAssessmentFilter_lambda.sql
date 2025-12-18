{{- config(materialized="view") -}}

{# Point the Provision to the Authority and Tenant of Direct Authority #}
{#
with batch as (
	select bat.*, 'batch' as part
	from {{ ref("AuthorityToAssessmentFilter_batch") }} bat
)
, stream as (
	select source.*, 'source' as part
	from {{ ref("vwAuthorityToAssessmentFilter_source") }} source
	where source.Filter_UpdateTime > ( select max(batch.Filter_UpdateTime) from batch )
)
, un as (
	select * from batch 
	union all 
	select * from stream
)
select * from stream
#}
select *
from {{ ref("vwAuthorityToAssessmentFilter_source") }}
