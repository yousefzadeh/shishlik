with base as(
select
    ap.Uuid union_id,
    ap.TenantId tenant_id,
    t.Name tenant_name,
    'Provision' record_type,
    'Authority:'+cast(ap.AuthorityId as varchar(12))+'/Provision:'+cast(ap.Id as varchar(12)) record_id,
    coalesce(ap.LastModificationTime, ap.creationTime) last_updatetime,
    null as status,
	REPLACE(REPLACE(
			(
			select 
				ha.AuthorityName,
				ha.ReferenceId, 
				ha.Name,
				--ha.CustomFieldId,
				--ha.CustomFieldName,
				--dbo.udf_StripHTML(ha.CustomFieldValue) CustomFieldValue,
			STRING_AGG('CustomField/'+ha.CustomFieldName + '":"' + ISNULL(dbo.udf_StripHTML(ha.CustomFieldValue), ''),'","') A
			from {{ ref("vwHaileyAuthorityProvisionjson") }} ha
			where ha.AuthorityProvisionId = ap.id
			GROUP BY ha.AuthorityName,ha.ReferenceId,Ha.Description
			order by ha.ReferenceId
			for json path, WITHOUT_ARRAY_WRAPPER ),'\',''),'"A":','') as text_data,
    --i.Embedding embedding,
     REPLACE(REPLACE(
        ( 
			select DISTINCT ha.CreationDate
			from {{ ref("vwHaileyAuthorityProvisionjson") }} ha
			where ha.AuthorityProvisionId = ap.id for json path), '[', ''), ']', '')  as additional_data

from  {{ source("hailey_models", "AuthorityProvision") }} ap
join  {{ source("hailey_models", "Authority") }} a
on a.Id = ap.AuthorityId
join {{ source("hailey_models", "AbpTenants") }} t
on t.Id = ap.TenantId
where ap.IsDeleted = 0
and t.IsDeleted = 0
and t.IsActive = 1

)
, main as (
select distinct
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
