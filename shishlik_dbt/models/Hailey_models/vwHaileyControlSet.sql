with base as (
select
    p.id,
    p.Uuid union_id,
    p.TenantId tenant_id,
    t.Name tenant_name,
    'ControlSet' record_type,
    'ControlSet:'+cast(p.Id as varchar(12)) record_id,
    coalesce(p.LastModificationTime, p.creationTime) last_updatetime,
    case
    when [status] = 1 then 'Edit'
    when [status] = 2 then 'Published'
    when [status] = 100 then 'Deprecated'
    else 'Undefined' end status,
     REPLACE(REPLACE(
        ( 
            select distinct
            hp.Name,
            dbo.udf_StripHTML(hp.Description) Description,
            hp.Owner,
            hp.Reviewer,
            hp.Reader,
            hp.Approver
        from {{ ref("vwHaileyControlSetjson") }} hp
        where hp.id = p.Id 
        order by hp.Name       
        for json path), '[', ''), ']', '') as text_data,
    --i.Embedding embedding,
     REPLACE(REPLACE(
        ( 
            select DISTINCT
            hp.CreationDate,
            hp.NextReviewDate,
            hp.LastReviewDate
        from {{ ref("vwHaileyControlSetjson") }} hp
        where hp.id = p.Id for json path), '[', ''), ']', '') as additional_data,
    case when lead(p.CreationTime) over (partition by coalesce(p.RootPolicyId,p.Id)  order by [Version]) is null then 1 else 0 end  IsCurrent

from {{ source("hailey_models", "Policy") }} p
join {{ source("hailey_models", "AbpTenants") }} t
on t.Id = p.TenantId
where p.IsDeleted = 0
and p.Status != 100
and t.IsDeleted = 0
and t.IsActive = 1
)

, main as (
select
    p.union_id,
    p.tenant_id,
    p.tenant_name,
    p.record_type,
    p.record_id,
    p.last_updatetime,
    p.status,
    p.text_data,
    --p.embedding,
  	CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', text_data), 2) AS text_hash,
    additional_data
from base p
where p.IsCurrent = 1
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

