with base as (
select
    r.Uuid union_id,
    r.TenantId tenant_id,
    t.Name tenant_name,
    'Risk' record_type,
    'Risk:'+cast(r.Id as varchar(12)) record_id,
    coalesce(r.LastModificationTime, r.creationTime) last_updatetime,
    wfs.Name status, 
     REPLACE(REPLACE(
        ( 
          select distinct
            hrj.Name,
            dbo.udf_StripHTML(hrj.Description) Description,
            hrj.CommonCause,
            hrj.LikelyImpact,
            hrj.IdRef,
            hrj.Domain,
            hrj.TreatmentStatus,
            hrj.TreatmentDecision,
            hrj.RiskRating,
            hrj.Tag,
            hrj.Owner,
            hrj.AccessMembers
          from {{ ref("vwHaileyRiskjson") }} hrj
          where hrj.id = r.Id  
          order by hrj.Name
          for json path), '[', ''), ']', '') as text_data,
    --i.Embedding embedding,
     REPLACE(REPLACE(
        ( 
          select hrj.CreationDate
          from {{ ref("vwHaileyRiskjson") }} hrj
          where hrj.id = r.Id for json path), '[', ''), ']', '') as additional_data

from {{ source("hailey_models", "Risk") }} r
join {{ source("hailey_models", "AbpTenants") }} t
on t.Id = r.TenantId
left join {{ source("hailey_models", "WorkflowStage") }} wfs
on wfs.Id = r.WorkflowStageId and wfs.IsDeleted = 0
where r.IsDeleted = 0
and t.IsDeleted = 0
and t.IsActive = 1
and r.Status = 1
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