with base as(
    select
        a.Uuid union_id,
        a.TenantId tenant_id,
        t.Name tenant_name,
        'Assessment' record_type,
        'Assessment:'+cast(a.Id as varchar(12)) record_id,
        coalesce(a.LastModificationTime, a.creationTime) last_updatetime,
        case
            when [Status] = 1 then 'Draft'
            when [Status] = 2 then 'Approved'
            when [Status] = 3 then 'Published'
            when [Status] = 4 then 'Completed'
            when [Status] = 5 then 'Closed'
            when [Status] = 6 then 'Reviewed'
            when [Status] = 7 then 'In Progress'
            when [Status] = 8 then 'Cancelled'
         else 'Undefined' end status,
     REPLACE(REPLACE(
        ( 
            select DISTINCT
            ha.Name,
            ha.Description,
            ha.Workflow,
            ha.AssessmentStyle,
            ha.Duedate,
            ha.Recurring,
            ha.Respondent,
            ha.Product,
            ha.Tag,
            ha.Owner,
            ha.AccessMembers
        from {{ ref("vwHaileyAssessmentjson") }} ha
        where ha.id = a.Id 
             order by ha.Name        
        for json path), '[', ''), ']', '') as text_data,
        --i.Embedding embedding,
     REPLACE(REPLACE(
        ( 
            select DISTINCT
            ha.CreationDate,
            ha.Duedate
        from {{ ref("vwHaileyAssessmentjson") }} ha
        where ha.id = a.Id for json path), '[', ''), ']', '') as additional_data

    from {{ source("hailey_models", "Assessment") }} a
    join {{ source("hailey_models", "AbpTenants") }} t
    on t.Id = a.TenantId
    where a.IsDeleted = 0
    and a.IsArchived = 0
    and a.IsTemplate = 0
    and a.IsDeprecatedAssessmentVersion = 0
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
--   where text_data is not null 
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