with longtext as (
select
ccd.ControlCustomFieldData_TenantId,
ccd.ControlCustomFieldData_Id,
ccd.ControlCustomFieldData_ControlId,
tpc.ThirdPartyControl_LabelVarchar Controls_CustomFieldName,
ccd.ControlCustomFieldData_TextValue Controls_CustomFieldValue
from {{ ref("vwControlCustomFieldData") }} ccd
left join {{ ref("vwThirdPartyControl") }} tpc
on tpc.ThirdPartyControl_Id = ccd.ControlCustomFieldData_ThirdPartyControlId
and tpc.ThirdPartyControl_Enabled = 1
where ccd.ControlCustomFieldData_TextValue is not null
and tpc.ThirdPartyControl_EntityType = 9
and tpc.ThirdPartyControl_Type = 6
)
, dropdown as(
select
ccd.ControlCustomFieldData_TenantId,
ccd.ControlCustomFieldData_Id,
ccd.ControlCustomFieldData_ControlId,
tpc.ThirdPartyControl_LabelVarchar Controls_CustomFieldName,
tpa.ThirdPartyAttributes_LabelVarchar Controls_CustomFieldValue
from {{ ref("vwControlCustomFieldData") }} ccd
left join {{ ref("vwThirdPartyControl") }} tpc
on tpc.ThirdPartyControl_Id = ccd.ControlCustomFieldData_ThirdPartyControlId
and tpc.ThirdPartyControl_Enabled = 1
left join {{ ref("vwThirdPartyAttributes") }} tpa
on tpa.ThirdPartyAttributes_Id = ccd.ControlCustomFieldData_ThirdPartyAttributeId
where tpc.ThirdPartyControl_EntityType = 9
and tpc.ThirdPartyControl_Type = 1
)
, ctrl_custom as (
select * from longtext
union all
select * from dropdown
)

select * from ctrl_custom