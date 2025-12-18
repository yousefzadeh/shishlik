select
union_id,
tenant_id,
tenant_name,
record_type,
record_id,
last_updatetime,
cast(status as varchar(120)) [status],
text_data,
text_hash,
--i.Embedding embedding,
additional_data
from {{ ref("vwHaileyAssessment_batch") }}

 union all

select
union_id,
tenant_id,
tenant_name,
record_type,
record_id,
last_updatetime,
cast(status as varchar(120)) [status],
text_data,
text_hash,
--i.Embedding embedding,
additional_data
from {{ ref("vwHaileyAsset_batch") }}

union all

select
union_id,
tenant_id,
tenant_name,
record_type,
record_id,
last_updatetime,
cast(status as varchar(120)) [status],
text_data,
text_hash,
--i.Embedding embedding,
additional_data
from {{ ref("vwHaileyAuthority_batch") }}

union all

select
union_id,
tenant_id,
tenant_name,
record_type,
record_id,
last_updatetime,
cast(status as varchar(120)) [status],
text_data,
text_hash,
--i.Embedding embedding,
additional_data
from {{ ref("vwHaileyAuthorityProvision_batch") }}

union all

select
union_id,
tenant_id,
tenant_name,
record_type,
record_id,
last_updatetime,
cast(status as varchar(120)) [status],
text_data,
text_hash,
--i.Embedding embedding,
additional_data
from {{ ref("vwHaileyControl_batch") }}

union all

select
union_id,
tenant_id,
tenant_name,
record_type,
record_id,
last_updatetime,
cast(status as varchar(120)) [status],
text_data,
text_hash,
--i.Embedding embedding,
additional_data
from {{ ref("vwHaileyControlSet_batch") }} 

union all

select
union_id,
tenant_id,
tenant_name,
record_type,
record_id,
last_updatetime,
cast(status as varchar(120)) [status],
text_data,
text_hash,
--i.Embedding embedding,
additional_data
from {{ ref("vwHaileyCustomRegister_batch") }}

union all

select
union_id,
tenant_id,
tenant_name,
record_type,
record_id,
last_updatetime,
cast(status as varchar(120)) [status],
text_data,
text_hash,
--i.Embedding embedding,
additional_data
from {{ ref("vwHaileyIssues_batch") }}

union all

select
union_id,
tenant_id,
tenant_name,
record_type,
record_id,
last_updatetime,
cast(status as varchar(120)) [status],
text_data,
text_hash,
--i.Embedding embedding,
additional_data
from {{ ref("vwHaileyProject_batch") }}

union all

select
union_id,
tenant_id,
tenant_name,
record_type,
record_id,
last_updatetime,
cast(status as varchar(120)) [status],
text_data,
text_hash,
--i.Embedding embedding,
additional_data
from {{ ref("vwHaileyRisk_batch") }}

union all

select
union_id,
tenant_id,
tenant_name,
record_type,
record_id,
last_updatetime,
cast(status as varchar(120)) [status],
text_data,
text_hash,
--i.Embedding embedding,
additional_data
from {{ ref("vwHaileyThirdParty_batch") }}