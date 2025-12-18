-- Asset Custom data for Attributes
select
    a.Asset_TenantId Tenant_Id,
    a.Asset_Id Id,
    a.Asset_Title Name,
    tpc.ThirdPartyControl_Id Custom_Id,
    tpc.ThirdPartyControl_Label Custom_Field,
    tpa.ThirdPartyAttributes_Label Custom_Field_Value
from {{ ref("vwAsset") }} a
inner join {{ ref("vwIssueCustomAttributeData") }} acad on acad.IssueCustomAttributeData_IssueId = a.Asset_Id
inner join
    {{ ref("vwThirdPartyAttributes") }} tpa
    on acad.IssueCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
inner join
    {{ ref("vwThirdPartyControl") }} tpc on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
where
    tpc.ThirdPartyControl_EntityType = 1
    and tpc.ThirdPartyControl_Enabled = 1
    and tpa.ThirdPartyAttributes_Label is not null
