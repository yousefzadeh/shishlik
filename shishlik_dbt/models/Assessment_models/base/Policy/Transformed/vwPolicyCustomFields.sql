select distinct
    p.Policy_TenantId,
    p.Policy_Id,
    p.Policy_Name,
    tpc.ThirdPartyControl_Label Policy_CustomField,
    tpa.ThirdPartyAttributes_Label Policy_CustomFieldValues,
    tpc.ThirdPartyControl_Type
from {{ ref("vwPolicy") }} p
left join {{ ref("vwPolicyCustomAttributeData") }} pcad on pcad.PolicyCustomAttributeData_PolicyId = p.Policy_Id
left join
    {{ ref("vwThirdPartyAttributes") }} tpa
    on tpa.ThirdPartyAttributes_Id = pcad.PolicyCustomAttributeData_ThirdPartyAttributesId
left join
    {{ ref("vwThirdPartyControl") }} tpc on tpc.ThirdPartyControl_Id = tpa.ThirdPartyAttributes_ThirdPartyControlId
where tpc.ThirdPartyControl_Type = 1  -- and p.Policy_Id = 3532
