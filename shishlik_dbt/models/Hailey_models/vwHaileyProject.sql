with base as (
select
    p.Uuid union_id,
    p.TenantId tenant_id,
    t.Name tenant_name,
    'Project' record_type,
    'Project:'+cast(p.Id as varchar(12)) record_id,
    coalesce(p.LastModificationTime, p.creationTime) last_updatetime,
    case 
        when p.status = 0 then 'Open'
        when p.status = 1 then 'Closed'
    else 'Undefined' end status,
     REPLACE(REPLACE(
        ( 
            select DISTINCT
                hp.Name,
                dbo.udf_StripHTML(hp.Description) Description,
                hp.owner,
                hp.tag
        from {{ ref("vwHaileyProjectjson") }} hp
        where hp.id = p.Id 
        order by hp.Name
        for json path), '[', ''), ']', '') as text_data,
    --i.Embedding embedding,
     REPLACE(REPLACE(
        ( 
            select hp.CreationDate
            from {{ ref("vwHaileyProjectjson") }} hp
            where hp.id = p.Id for json path), '[', ''), ']', '') as additional_data

from {{ source("hailey_models", "Project") }} p
join {{ source("hailey_models", "AbpTenants") }} t
on t.Id = p.TenantId
where p.IsDeleted = 0
and p.IsArchived = 0
and p.IsTemplate = 0
and t.IsDeleted = 0
and t.IsActive = 1
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
