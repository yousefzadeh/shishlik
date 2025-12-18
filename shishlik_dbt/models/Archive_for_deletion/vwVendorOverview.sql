{{ config(materialized="view") }}

with
    vend_overview as (
        select
            tv.TenantVendor_Id,
            tv.TenantVendor_Geography,
            tv.TenantVendor_Industry,
            tv.TenantVendor_IsArchived,
            tv.TenantVendor_Name,
            e.Engagement_Id,
            e.Engagement_Name,
            e.Engagement_Description,
            e.Engagement_BusinessUnit,
            e.Engagement_Criticality,
            e.Engagement_InherentRisk,
            tpftcd.ThirdPartyFreeTextControlData_TextData,
            tpftcd.ThirdPartyFreeTextControlData_CustomDateValue,
            tpa.ThirdPartyAttributes_Label,
            tpa.ThirdPartyAttributes_Description,
            tpdfd.ThirdPartyDynamicFieldData_XAxisAttributeLabel,
            tpdfd.ThirdPartyDynamicFieldData_YAxisAttributeLabel,
            tpdfd.ThirdPartyDynamicFieldData_DynamicValue,
            tpdfd.ThirdPartyDynamicFieldData_Description,
            tpdfd.ThirdPartyDynamicFieldData_DynamicScoreValue,
            tpdfd.ThirdPartyDynamicFieldData_DynamicColor

        from {{ ref("vwTenantVendor") }} tv
        join {{ ref("vwEngagement") }} e on tv.TenantVendor_TenantId = e.Engagement_TenantId
        join
            {{ ref("vwThirdPartyFreeTextControlData") }} tpftcd
            on tv.TenantVendor_Id = tpftcd.ThirdPartyFreeTextControlData_TenantVendorId
        join {{ ref("vwThirdPartyData") }} tpd on tv.TenantVendor_Id = tpd.ThirdPartyData_TenantVendorId
        join
            {{ ref("vwThirdPartyAttributes") }} tpa
            on tpa.ThirdPartyAttributes_Id = tpd.ThirdPartyData_ThirdPartyAttributesId
        join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpc.ThirdPartyControl_Id = tpftcd.ThirdPartyFreeTextControlData_ThirdPartyControlId
        join
            {{ ref("vwThirdPartyDynamicFieldConfiguration") }} tpdfc
            on tpc.ThirdPartyControl_Id = tpdfc.ThirdPartyDynamicFieldConfiguration_ThirdPartyControlId
        join
            {{ ref("vwThirdPartyDynamicFieldData") }} tpdfd
            on tpdfd.ThirdPartyDynamicFieldData_ThirdPartyDynamicFieldConfigurationId
            = tpdfc.ThirdPartyDynamicFieldConfiguration_Id

    )

select *
from vend_overview
