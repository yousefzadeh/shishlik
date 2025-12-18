{{ config(materialized="view") -}}
{# Point the Provision to the Authority and Tenant of Direct Authority #}
{# with 
 batch as (
	select *
	from {{ ref("ProvisionCustomFieldValue_batch") }} 
)
------- Custom Source -------
, acf_raw as ( -- Authority Custom Attribute (Field) 
	SELECT
		[Id] AS [AuthorityProvisionCustomField_Id]
		, [UpdateTime] AS [AuthorityProvisionCustomField_UpdateTime]
		, [AuthorityId] AS [AuthorityProvisionCustomField_AuthorityId]
		, cast([FieldName] as nvarchar(200)) AS [AuthorityProvisionCustomField_FieldName]
		, cast([FieldType] as nvarchar(200)) AS [AuthorityProvisionCustomField_FieldType]
		, [Order] AS [AuthorityProvisionCustomField_Order]
	FROM
		{{ source("assessment_models","AuthorityProvisionCustomField") }}
	where
		IsDeleted = 0 
		and UpdateTime > ( select max(batch.ProvisionCustomFieldValue_UpdateTime) from batch )
)
, acf_null as (
	select 
	0 AuthorityProvisionCustomField_Id,
	cast('2000-01-01' as datetime) AuthorityProvisionCustomField_UpdateTime,
	0 AuthorityProvisionCustomField_AuthorityId,
	'Unassigned' AuthorityProvisionCustomField_FieldName, 
	'0' AuthorityProvisionCustomField_FieldType, 
	1 AuthorityProvisionCustomField_Order
)
, acf as (
	select * from acf_raw 
	union all 
	select * from acf_null
)
, ap_raw as ( -- Provision table with Custom Attribute and Values in CustomJson 
	select distinct 
	ap.AuthorityId AuthorityProvision_AuthorityId,
	ap.Id AuthorityProvision_Id,
	cast(ap.ReferenceId as nvarchar(100)) AuthorityProvision_ReferenceId,
	cast(ap.Name as nvarchar(4000)) AuthorityProvision_Name,
	ap.CustomDataJson AuthorityProvision_CustomDataJson,
	UpdateTime as AuthorityProvision_UpdateTime,
	getdate() as sys_update_time
	from {{ source('assessment_models','AuthorityProvision') }} ap
	where IsDeleted = 0
	and UpdateTime > ( select max(batch.ProvisionCustomFieldValue_UpdateTime) from batch )
)
, apcustom_null as (	
	-- Authority with no Provision
	select 
	AuthorityProvisionCustomField_AuthorityId AuthorityProvision_AuthorityId,
	0 AuthorityProvision_Id,
	'0' AuthorityProvision_ReferenceId,
	'Unassigned Provision' AuthorityProvision_Name,
	0 as AuthorityProvisionCustom_ID, 
	cast(AuthorityProvisionCustomField_FieldName as varchar(800)) as AuthorityProvisionCustom_Field,
	cast('Unassigned Value' as varchar(800)) as AuthorityProvisionCustom_Value,
	AuthorityProvisionCustomField_UpdateTime as AuthorityProvision_UpdateTime
	from acf_raw

	union all 

	-- No Authority and No Provision
	select 
	0 AuthorityProvision_AuthorityId,
	0 AuthorityProvision_Id,
	'' AuthorityProvision_ReferenceId,
	'Unassigned Authority and Provision' AuthorityProvision_Name,
	0 as AuthorityProvisionCustom_ID, 
	cast('Unassigned Field' as varchar(800)) as AuthorityProvisionCustom_Field,
	cast('Unassigned Value' as varchar(800)) as AuthorityProvisionCustom_Value,
	cast('2000-01-01' as datetime) AuthorityProvision_UpdateTime
)
, apcustom as (	-- Provision with Custom Attributes and Values in rows 
	select 
	AuthorityProvision_AuthorityId,
	AuthorityProvision_Id,
	AuthorityProvision_ReferenceId,
	AuthorityProvision_Name,
	-- Unnested JSON columns in rows --
	coalesce(cast(c.[key] as int)+1,0) as AuthorityProvisionCustom_ID, -- 0 is null, 1..n 
	-- Internal key no reference to Authority Custom 
	-- cast(json_value(c.[value],'$.Id') as INT) as AuthorityProvisionCustom_ID, 
	cast(json_value(c.[value],'$.Name') as varchar(800)) as AuthorityProvisionCustom_Field,
	cast(json_value(c.[value],'$.Value') as varchar(800)) as AuthorityProvisionCustom_Value,
	AuthorityProvision_UpdateTime
	from ap_raw 
	OUTER APPLY OPENJSON(ap_raw.AuthorityProvision_CustomDataJson) as c
	-- Filter out null values in Provision level Field and Values to make table smaller 
	where cast(coalesce(json_value(c.[value],'$.Name'),'') as varchar(800)) <> ''
	--  and cast(coalesce(json_value(c.[value],'$.Value'),'') as varchar(800)) <> ''  
	
	union all 

	select *
	from apcustom_null
)
, apcustom_clean as (
	select 
	AuthorityProvision_AuthorityId,
	coalesce(AuthorityProvision_Id,0) AuthorityProvision_Id,
	AuthorityProvision_ReferenceId,
	AuthorityProvision_Name,
	coalesce(AuthorityProvisionCustom_ID,0) AuthorityProvisionCustom_ID, 
	AuthorityProvisionCustom_Field,
	replace(coalesce(AuthorityProvisionCustom_Value,'Blank'),'<br>','&nbsp;') AuthorityProvisionCustom_Value,
	AuthorityProvision_UpdateTime
    from apcustom
)
, auth_prov_custom_field_value as ( -- All Custom Attributes with Custom Values (If Any) 
	select 
	acf.AuthorityProvisionCustomField_Id           AuthorityCustom_Id,
	acf.AuthorityProvisionCustomField_AuthorityId  AuthorityCustom_AuthorityId,
	acf.AuthorityProvisionCustomField_FieldName    AuthorityCustom_FieldName,
	acf.AuthorityProvisionCustomField_Order        AuthorityCustom_FieldOrder,
	pcfv.AuthorityProvision_Id                     Provision_Id,
	                                               -- Unnested JSON 
	pcfv.AuthorityProvisionCustom_ID               ProvisionCustom_ValueId,
	pcfv.AuthorityProvisionCustom_Value            ProvisionCustom_Value
	from acf                  -- Authority Custom Field 
	join apcustom_clean pcfv  -- Provision Custom Field Value 
	  on  acf.AuthorityProvisionCustomField_Order = AuthorityProvisionCustom_ID
	  and acf.AuthorityProvisionCustomField_AuthorityId = pcfv.AuthorityProvision_AuthorityId
)
, source as (
	select 
	T.*,
	-- PK --
	'A'+cast(AuthorityCustom_AuthorityId as varchar(10))+
	'P'+cast(Provision_Id as varchar(10))+
	'@'+cast(AuthorityCustom_FieldOrder as varchar(5))+
	'='+cast(coalesce(ProvisionCustom_ValueId,0) as varchar(5)) as ProvisionCustomFieldValue_PK
	from auth_prov_custom_field_value T
)
-------- 
, 
stream as (
	select *
	from source
	-- where source.ProvisionCustomFieldValue_UpdateTime > ( select max(batch.ProvisionCustomFieldValue_UpdateTime) from batch )
)
select 
AuthorityCustom_Id
, AuthorityCustom_AuthorityId
, AuthorityCustom_FieldName
, AuthorityCustom_FieldOrder
, Provision_Id
, ProvisionCustom_ValueId
, ProvisionCustom_Value
, ProvisionCustomFieldValue_PK
from batch 
union all 
select 
AuthorityCustom_Id
, AuthorityCustom_AuthorityId
, AuthorityCustom_FieldName
, AuthorityCustom_FieldOrder
, Provision_Id
, ProvisionCustom_ValueId
, ProvisionCustom_Value
, ProvisionCustomFieldValue_PK
from stream
#}
select *
from {{ ref("vwProvisionCustomFieldValue_source") }}
