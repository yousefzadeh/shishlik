select
rm.TenantId,
rm.Matrix_Id,
rm.Matrix_Name,
rx.X_Axis,
rx.X_Attribute_Id,
rx.X_Attribute,
rx.X_Order,
ry.y_Axis,
ry.y_Attribute_Id,
ry.y_Attribute,
ry.y_Order,
tpdfd.DynamicScoreValue,
tpdfd.DynamicColor,
tpdfd.DynamicValue,
tpdfd.DynamicValueOptionId

from {{ ref("vRiskMatrixName") }} rm
join {{ ref("vRiskMatrixXaxis") }} rx
on rx.Matrix_Id = rm.matrix_Id
and rx.Config_Id = rm.Config_Id
and rx.TenantId = rm.TenantId
join {{ ref("vRiskMatrixYaxis") }} ry
on ry.Matrix_Id = rm.matrix_Id
and ry.Config_Id = rm.Config_Id
and ry.TenantId = rm.TenantId
join {{ source("third-party_ref_models", "ThirdPartyDynamicFieldData") }} as tpdfd
on rm.Config_Id = tpdfd.ThirdPartyDynamicFieldConfigurationId
and tpdfd.XAxisAttributeId = rx.X_Attribute_Id
and tpdfd.YAxisAttributeId = ry.Y_Attribute_Id
where rm.TenantId is not null