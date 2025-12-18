with base as(
        select
        iss.Uuid union_id,
        iss.TenantId tenant_id,
        t.Name tenant_name,
        'Asset' record_type,
        'Asset:'+cast(iss.Id as varchar(12)) record_id,
        coalesce(iss.LastModificationTime, iss.creationTime) last_updatetime,
        wfs.Name status,
     REPLACE(REPLACE(
        ( 
            select DISTINCT 
            ha.Name,
            dbo.udf_StripHTML(ha.Description) Description,
            ha.HostIP,
            ha.Domain,
            ha.Type,
            ha.Tag,
            ha.ThirdParty,
            ha.Owner,
            ha.AccessMembers
        from {{ ref("vwHaileyAssetjson") }} ha
        where ha.id = iss.Id 
             order by ha.Name       
        for json path), '[', ''), ']', '')  as text_data,
    --i.Embedding embedding,
     REPLACE(REPLACE(
        ( 
            select ha.CreationDate from {{ ref("vwHaileyAssetjson") }} ha
            where ha.id = iss.Id for json path), '[', ''), ']', '')  as additional_data
    from {{ source("hailey_models", "Issues") }} iss
    join {{ source("hailey_models", "EntityRegister") }} er
    on er.Id = iss.EntityRegisterId and er.IsDeleted = 0 and er.EntityType = 5  
    join {{ source("hailey_models", "AbpTenants") }} t
    on t.Id = iss.TenantId
    left join {{ source("hailey_models", "WorkflowStage") }} wfs
    on wfs.Id = iss.WorkflowStageId and wfs.IsDeleted = 0
    where iss.IsDeleted = 0
    and t.IsDeleted = 0
    and t.IsActive = 1
    and iss.Status != 100
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