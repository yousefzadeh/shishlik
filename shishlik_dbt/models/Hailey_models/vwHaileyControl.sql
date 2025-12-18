with base as (
select
    p.id,
    c.Uuid union_id,
    c.TenantId tenant_id,
    t.Name tenant_name,
    'Control' record_type,
    'ControlSet:'+cast(p.Id as varchar(12))+'/Control:'+cast(c.Id as varchar(12)) record_id,
    coalesce(c.LastModificationTime, c.creationTime) last_updatetime,
    cast(NULL as varchar(1)) status,
     REPLACE(REPLACE(
        ( 
            select DISTINCT  
            hc.Name,
            dbo.udf_StripHTML(hc.Description) Description,
            hc.Reference,
            hc.PolicyName,
            hc.PolicyDomainName,
            hc.Owner
            from {{ ref("vwHaileyControljson") }} hc
            where hc.id = c.Id 
            order by hc.Name
            for json path), '[', ''), ']', '') as text_data, 
        --i.Embedding embedding,
     REPLACE(REPLACE(
        ( 
            select  hc.CreationDate from {{ ref("vwHaileyControljson") }} hc
            where hc.id = c.Id for json path), '[', ''), ']', '') as additional_data,
    case
    when lead(c.[CreationTime]) over (partition by coalesce(c.[RootControlId], c.[Id]) order by c.[CreationTime]) is null then 1
    else 0 end IsCurrent

from {{ source("hailey_models", "Policy") }} p
join {{ source("hailey_models", "AbpTenants") }} t
on t.Id = p.TenantId
join {{ source("hailey_models", "PolicyDomain") }} pd
on pd.PolicyId = p.Id and pd.IsDeleted = 0
join {{ source("hailey_models", "Controls") }} c
on c.PolicyDomainId = pd.Id and c.IsDeleted = 0
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
-- and  p.text_data is not null 
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