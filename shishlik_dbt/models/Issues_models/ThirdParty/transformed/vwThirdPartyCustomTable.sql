{{ config(materialized="view") }}

with
    custom_select_data as (
        select
            tpd.ThirdPartyData_TenantVendorId TenantVendor_Id,
            cast(tpa.ThirdPartyAttributes_Label as varchar(100)) CustomFieldValue,
            cast(tpc.ThirdPartyControl_Label as varchar(100)) CustomFieldName,
            tpa.ThirdPartyAttributes_UpdateTime as ThirdPartyCustomTable_UpdateTime
        from {{ ref("vwThirdPartyData") }} tpd
        join
            {{ ref("vwThirdPartyAttributes") }} tpa
            on tpd.ThirdPartyData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
        join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
        where tpc.ThirdPartyControl_Type in (1,2,4)
    ),
    custom_value_data as (
        select
            tpd.ThirdPartyFreeTextControlData_TenantVendorId TenantVendor_Id,
            case 
            when tpc.ThirdPartyControl_Type in (3,6)
            then cast(tpd.ThirdPartyFreeTextControlData_TextData as varchar(100)) 
            when tpc.ThirdPartyControl_Type = 5 
            then cast(tpd.ThirdPartyFreeTextControlData_CustomDateValue as varchar(100))
            when tpc.ThirdPartyControl_Type = 7 
            then cast(tpd.ThirdPartyFreeTextControlData_NumberValue as varchar(100)) 
            end CustomFieldValue,
            cast(tpc.ThirdPartyControl_Label as varchar(100)) CustomFieldName,
            tpd.ThirdPartyFreeTextControlData_UpdateTime as ThirdPartyCustomTable_UpdateTime
        from {{ ref("vwThirdPartyFreeTextControlData") }} tpd
        join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpd.ThirdPartyFreeTextControlData_ThirdPartyControlId = tpc.ThirdPartyControl_Id
        where tpc.ThirdPartyControl_Type in (3,5,6,7)
    ),
    final as (
	select  distinct	TenantVendor_Id,	CustomFieldValue,	CustomFieldName
	, max(ThirdPartyCustomTable_UpdateTime)over(partition by TenantVendor_Id,	CustomFieldValue,	CustomFieldName)as ThirdPartyCustomTable_UpdateTime
	from (
        select * from custom_select_data
        union 
        select * from custom_value_data
    )a
	)
select *
	, rank() over (order by TenantVendor_Id, CustomFieldValue,CustomFieldName) as ThirdPartyCustomTable_pk
from final
{# where TenantVendor_Id = 20075 #}