select distinct
    tpc.ThirdPartyControl_TenantId Spoke_Id,  -- join to spoke 
    tpc.ThirdPartyControl_Label FieldName,
    tpc.ThirdPartyControl_TypeCode FieldType,
    tpa.ThirdPartyAttributes_Label FieldValue
-- Custom Field and all Values
from {{ ref("vwThirdPartyControl") }} tpc  -- Field
join
    {{ ref("vwThirdPartyAttributes") }} tpa  -- value 
    on tpc.ThirdPartyControl_Id = tpa.ThirdPartyAttributes_ThirdPartyControlId
-- Selected value
join {{ ref("vwTenantVendor") }} tv on tv.TenantVendor_TenantId = tpc.ThirdPartyControl_TenantId
join
    {{ ref("vwThirdPartyData") }} tpd  -- selected value for a ThirdParty
    on tpd.ThirdPartyData_TenantVendorId = tv.TenantVendor_Id
    and tpd.ThirdPartyData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
-- Third Party type only and enabled
where
    tpc.ThirdPartyControl_EntityType = 0
    and tpc.ThirdPartyControl_Enabled = 1
