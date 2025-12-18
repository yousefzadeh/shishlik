with base as (
select
    tv.Uuid union_id,
    tv.TenantId tenant_id,
    t.Name tenant_name,
    'Third-Party' record_type,
    tv.Id TenantVendor_Id,
    'Third-Party:'+cast(tv.Id as varchar(12)) record_id,
    coalesce(tv.LastModificationTime, tv.creationTime) last_updatetime,
    REPLACE(REPLACE(
    ( 
        select distinct
            ht.Name,
            ht.ContactEmail,
            ht.Website,
            ht.RiskRating,
            ht.Criticality,
            ht.Geography,
            ht.Industry,
            ht.Tag,
            ht.Owner
        from {{ ref("vwHaileyThirdPartyjson") }} ht
        where ht.id = tv.Id 
        order by ht.Name,ht.ContactEmail
        for json path), '[', ''), ']', '') as text_data,
    --i.Embedding embedding,
     REPLACE(REPLACE(
        ( 
            select ht.CreationDate
            from {{ ref("vwHaileyThirdPartyjson") }} ht
            where ht.id = tv.Id for json path), '[', ''), ']', '') as additional_data

from {{ source("hailey_models", "TenantVendor") }} tv
join {{ source("hailey_models", "AbpTenants") }} t
on t.Id = tv.TenantId
where tv.IsDeleted = 0
and tv.IsArchived = 0
and t.IsDeleted = 0
and t.IsActive = 1
)
, tp_stag as(
select
    tpd.TenantVendorId,
    tpc.Label ThirdParty_Stage,
    tpa.LabelVarchar status
from {{ source("hailey_models", "ThirdPartyData") }} tpd
join {{ source("hailey_models", "ThirdPartyAttributes") }} tpa
on tpa.Id = tpd.ThirdPartyAttributesId and tpa.IsDeleted = 0
join {{ source("hailey_models", "ThirdPartyControl") }} tpc
on tpc.Id = tpa.ThirdPartyControlId and tpc.IsDeleted = 0
where tpd.IsDeleted = 0 and tpc.Name = 'Stage'
)

, main as (
select
    tv.union_id,
    tv.tenant_id,
    tv.tenant_name,
    tv.record_type,
    tv.record_id,
    tv.last_updatetime,
    ts.status,
    tv.text_data,
  	CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', tv.text_data), 2) AS text_hash,
    --embedding,
    additional_data
from base tv
left join tp_stag ts on ts.TenantVendorId = tv.TenantVendor_Id
--    where tv.text_data is not null 
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