{{ config(materialized="view") }}

select *
from {{ ref("vwThirdPartyData") }} tpd
inner join
    {{ ref("vwThirdPartyAttributes") }} tpa on tpd.ThirdPartyData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
inner join
    {{ ref("vwThirdPartyControl") }} tpc on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
where tpc.ThirdPartyControl_EntityType = 0
