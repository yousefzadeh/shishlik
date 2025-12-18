with
    x_value as (
        select
            COALESCE(at2.AbpTenants_Id, tpc.TenantId) Tenant_Id,
            tpc.Id,
            tpc.Label x_axis_name,
            tpa.Id x_attribute_id,
            tpa.Label x_attribute_name,
            tpa.Value x_order,
            tpa.Description x_attribute_description
        from {{ source("issue_models", "ThirdPartyControl") }} tpc
        join {{ source("issue_models", "ThirdPartyAttributes") }} tpa on tpc.Id = tpa.ThirdPartyControlId
        left outer hash join {{ ref("vwAbpTenants") }} at2 on at2.AbpTenants_ServiceProviderId = tpc.TenantId
        where
            1 = 1
            -- and tpc.Label = 'Likelihood' 
            -- and tpc.TenantId = 4980 
            and tpc.IsDeleted = 0
            and tpc.Enabled = 1
            and tpa.IsDeleted = 0
    ),
    x_axis as (
        select distinct
            tpdfc.ThirdPartyDynamicFieldConfiguration_Id tpdfc_Id,
            vtpc.ThirdPartyControl_Label MatrixName,
            coalesce(at2.AbpTenants_Id, tpdfc.ThirdPartyDynamicFieldConfiguration_TenantId) Tenant_Id,
            tpdfc.ThirdPartyDynamicFieldConfiguration_ThirdPartyControlId x_axis_id,
            -- tpdfc.ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlName x_axis_name,
            tpc.ThirdPartyControl_Label x_axis_name,
            tpdfc.ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlId
        from {{ ref("vwThirdPartyDynamicFieldConfiguration") }} tpdfc
        join
            {{ ref("vwThirdPartyControl") }} vtpc
            on vtpc.ThirdPartyControl_Id = tpdfc.ThirdPartyDynamicFieldConfiguration_ThirdPartyControlId
        left join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpc.ThirdPartyControl_Id = tpdfc.ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlId
        left join
            {{ ref("vwAbpTenants") }} at2
            on at2.AbpTenants_ServiceProviderId = tpdfc.ThirdPartyDynamicFieldConfiguration_TenantId
    ),
    x_axis_value as (
        select
            x_axis.*, x_value.x_attribute_id, x_value.x_attribute_name, x_value.x_order, x_value.x_attribute_description
        from x_axis
        join
            x_value
            on x_axis.Tenant_id = x_value.Tenant_Id
            and x_axis.ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlId = x_value.Id
    ),
    y_value as (
        select
            COALESCE(at2.AbpTenants_Id, tpc.TenantId) Tenant_Id,
            tpc.Id,
            tpc.Label y_axis_name,
            tpa.Id y_attribute_id,
            tpa.Label y_attribute_name,
            tpa.Value y_order,
            tpa.Description y_attribute_description
        from {{ source("issue_models", "ThirdPartyControl") }} tpc
        join {{ source("issue_models", "ThirdPartyAttributes") }} tpa on tpc.Id = tpa.ThirdPartyControlId
        left join {{ ref("vwAbpTenants") }} at2 on at2.AbpTenants_ServiceProviderId = tpc.TenantId
        where
            1 = 1
            -- and tpc.Label = 'Impact' 
            -- and tpc.TenantId = 4980 
            and tpc.IsDeleted = 0
            and tpc.Enabled = 1
            and tpa.IsDeleted = 0
    ),
    y_axis as (
        select distinct
            tpdfc.ThirdPartyDynamicFieldConfiguration_Id tpdfc_Id,
            vtpc.ThirdPartyControl_Label MatrixName,
            coalesce(at2.AbpTenants_Id, tpdfc.ThirdPartyDynamicFieldConfiguration_TenantId) Tenant_Id,
            tpdfc.ThirdPartyDynamicFieldConfiguration_ThirdPartyControlId y_axis_id,
            -- tpdfc.ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlName y_axis_name,
            tpc.ThirdPartyControl_Label y_axis_name,
            tpdfc.ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlId
        from {{ ref("vwThirdPartyDynamicFieldConfiguration") }} tpdfc
        join
            {{ ref("vwThirdPartyControl") }} vtpc
            on vtpc.ThirdPartyControl_Id = tpdfc.ThirdPartyDynamicFieldConfiguration_ThirdPartyControlId
        left join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpc.ThirdPartyControl_Id = tpdfc.ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlId
        left join
            {{ ref("vwAbpTenants") }} at2
            on at2.AbpTenants_ServiceProviderId = tpdfc.ThirdPartyDynamicFieldConfiguration_TenantId
    ),
    y_axis_value as (
        select
            y_axis.*, y_value.y_attribute_id, y_value.y_attribute_name, y_value.y_order, y_value.y_attribute_description
        from y_axis
        join
            y_value
            on y_axis.Tenant_id = y_value.Tenant_Id
            and y_axis.ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlId = y_value.Id
    )
select distinct
    x.MatrixName,
    x_axis_id ThirdPartyControl_Id,
    -- 'Risk Rating' ThirdPartyControl_Label,
    x.Tenant_Id ThirdPartyDynamicFieldConfiguration_TenantId,
    x.ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlId,
    x.x_axis_name ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlName,
    y.ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlId,
    y.y_axis_name ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlName,
    x.x_attribute_id,
    x.x_attribute_name ThirdPartyDynamicFieldData_XAxisAttributeLabel,
    y.y_attribute_id,
    y.y_attribute_name ThirdPartyDynamicFieldData_YAxisAttributeLabel,
    x.x_order XLabel_order,
    y.y_order YLabel_order,
    tpdfd.ThirdPartyDynamicFieldData_DynamicScoreValue,
    tpdfd.ThirdPartyDynamicFieldData_DynamicColor,
    tpdfd.ThirdPartyDynamicFieldData_DynamicValue
from x_axis_value x
join y_axis_value y on x.Tenant_Id = y.Tenant_Id
join
    {{ ref("vwThirdPartyDynamicFieldData") }} as tpdfd
    on x.tpdfc_Id = tpdfd.ThirdPartyDynamicFieldData_ThirdPartyDynamicFieldConfigurationId
    and tpdfd.ThirdPartyDynamicFieldData_XAxisAttributeId = x.x_attribute_id
    and tpdfd.ThirdPartyDynamicFieldData_YAxisAttributeId = y.y_attribute_id
    -- where x.Tenant_id = 4980
    
