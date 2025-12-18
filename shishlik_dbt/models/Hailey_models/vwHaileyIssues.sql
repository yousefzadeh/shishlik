with base as (
select
    i.Uuid union_id,
    i.TenantId tenant_id,
    t.Name tenant_name,
    'Issue' record_type,
    'Issue:'+cast(i.Id as varchar(12)) record_id,
    coalesce(i.LastModificationTime, i.creationTime) last_updatetime,
    wfs.Name status,
       REPLACE(REPLACE(
        ( 
          select distinct
            hij.IdRef,
            hij.Name,
            dbo.udf_StripHTML(hij.Description) Description,
            hij.IdentifiedBy,
            hij.Priority,
            hij.Type,
            hij.Tag,
            hij.Owner,
            hij.AccessMembers
          from {{ ref("vwHaileyIssuesjson") }} hij
          where hij.id = i.Id 
          order by hij.IdRef,hij.Name
          for json path), '[', ''), ']', '') as text_data,
    --i.Embedding embedding,
     REPLACE(REPLACE(
        (  
          select distinct
            hij.CreationDate,
            hij.RecordedDate
          from {{ ref("vwHaileyIssuesjson") }} hij
          where hij.id = i.Id for json path), '[', ''), ']', '') as  additional_data

from {{ source("hailey_models", "Issues") }} i
join {{ source("hailey_models", "AbpTenants") }} t
on t.Id = i.TenantId
join {{ source("hailey_models", "EntityRegister") }} er
on er.Id = i.EntityRegisterId and er.IsDeleted = 0  and er.EntityType = 3
left join {{ source("hailey_models", "WorkflowStage") }} wfs
on wfs.Id = i.WorkflowStageId and wfs.IsDeleted = 0
where i.IsDeleted = 0
and t.IsDeleted = 0
and t.IsActive = 1
and i.IsArchived = 0
and i.Status != 100
)
, main as (
select 
	union_id,
	tenant_id,
	tenant_name,
	record_type,
	record_id,
	last_updatetime,
	status,
	text_data,
  	CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', text_data), 2) AS text_hash,
	additional_data
 from base
) 
select 
	union_id,
	tenant_id,
	tenant_name,
	record_type,
	record_id,
	last_updatetime,
	status,
	text_data,
	text_hash,
	additional_data
from main