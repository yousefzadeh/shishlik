with base as(
select
    a.Uuid union_id,
    a.TenantId tenant_id,
    t.Name tenant_name,
    'Authority' record_type,
    'Authority:'+cast(a.Id as varchar(12)) record_id,
    coalesce(a.LastModificationTime, a.creationTime) last_updatetime,
    case when a.Status = 1 then 'Edit' when a.Status = 2 then 'Published' else 'Undefined' end status,
     REPLACE(REPLACE(
        (select DISTINCT 
            ha.Name,
            dbo.udf_StripHTML(ha.Description) Description,
            ha.AuthorityBody,
            ha.AuthoritySector,
            ha.AuthorityReferenceURL,
            ha.type,
            ha.jurisdiction,
            ha.owner
        from {{ ref("vwHaileyAuthorityjson") }} ha
        where ha.id = a.Id 
            order by ha.Name        
        for json path), '[', ''), ']', '') as text_data,
    --i.Embedding embedding,
     REPLACE(REPLACE(
        (  
            select  ha.CreationDate from {{ ref("vwHaileyAuthorityjson") }} ha
            where ha.id = a.Id for json path), '[', ''), ']', '') as additional_data

from {{ source("hailey_models", "Authority") }} a
join {{ source("hailey_models", "AbpTenants") }} t
on t.Id = a.TenantId
where a.IsDeleted = 0
and a.IsArchived = 0
and t.IsDeleted = 0
and t.IsActive = 1

union all

select
    ta.Uuid union_id,
    ta.TenantId tenant_id,
    t.Name tenant_name,
    'Authority' record_type,
    'Authority:'+cast(ta.AuthorityId as varchar(12)) record_id,
    coalesce(ta.LastModificationTime, ta.creationTime) last_updatetime,
    case when a.Status = 1 then 'Edit' when a.Status = 2 then 'Published' else 'Undefined' end status,
     REPLACE(REPLACE(
        ( 
            select DISTINCT 
                ha.Name,
                dbo.udf_StripHTML(ha.Description) Description,
                ha.AuthorityBody,
                ha.AuthoritySector,
                ha.AuthorityReferenceURL,
                ha.type,
                ha.jurisdiction,
                ha.owner
            from {{ ref("vwHaileyAuthorityjson") }} ha
            where ha.id = ta.AuthorityId 
            order by ha.Name 
            for json path), '[', ''), ']', '') as text_data,
    --i.Embedding embedding,
     REPLACE(REPLACE(
        ( 
            select ha.CreationDate from {{ ref("vwHaileyAuthorityjson") }} ha
            where ha.id = ta.AuthorityId for json path), '[', ''), ']', '') as additional_data

from {{ source("hailey_models", "TenantAuthority") }} ta
join {{ source("hailey_models", "Authority") }} a
on ta.AuthorityId = a.Id
join {{ source("hailey_models", "AbpTenants") }} t
on t.Id = ta.TenantId
where ta.IsDeleted = 0
and ta.IsArchived = 0
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