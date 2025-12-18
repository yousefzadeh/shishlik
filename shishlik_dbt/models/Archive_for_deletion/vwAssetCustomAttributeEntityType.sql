{{ config(materialized="view") }}

select *
from {{ ref("vwAsset") }} a
inner join {{ ref("vwAssetCustomAttributeData") }} acad on acad.AssetCustomAttributeData_AssetId = a.Asset_Id
inner join
    {{ ref("vwThirdPartyAttributes") }} tpa
    on acad.AssetCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
inner join
    {{ ref("vwThirdPartyControl") }} tpc on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
where tpc.ThirdPartyControl_EntityType = 1
