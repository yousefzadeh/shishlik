{{ config(materialized="view") }}

-- The list of possible attribute values for dropdown, multiselect dropdown, and
-- dynamic attributes are stored in the
-- ThirdPartyAttributes table. This table is also used for other entity types.
-- The actual attribute values for risks are stored in two tables:
-- * For dropdown, multiselect dropdown, and dynamic attributes, the values are stored
-- in the RiskCustomAttributeData
-- table.
-- * For free-text, rich-text, and date attributes, the values are stored in the
-- RiskThirdPartyControlCustomText
-- table. For free-text and rich-text attributes, the value is stored in the TextData
-- column. For date attributes, the
-- value is stored in the CustomDateValue.
-- The RiskCustomAttributeData and RiskThirdPartyControlCustomText tables are only
-- used for risk custom attributes.
-- Other entity types (e.g. issues, third-parties) have their own corresponding tables.
with
    rtpcct as (
        -- Attribute values for free text
        select
            RiskThirdPartyControlCustomText_TenantId,
            RiskThirdPartyControlCustomText_RiskId,
            RiskThirdPartyControlCustomText_TextData,
            RiskThirdPartyControlCustomText_CustomDateValue,
            RiskThirdPartyControlCustomText_NumberValue,
            RiskThirdPartyControlCustomText_ThirdPartyControlId
        from {{ ref("vwRiskThirdPartyControlCustomText") }}
    ),
    tpc as (
        -- Attributes by entity
        select ThirdPartyControl_Id, ThirdPartyControl_Label, ThirdPartyControl_TenantId
        from {{ ref("vwThirdPartyControl") }}
    ),
    rtpcct_tpc as (
        -- Issue_Id, Attribute label, attribute value - free text or date value
        select
            rtpcct.RiskThirdPartyControlCustomText_TenantId Risk_TenantId,
            rtpcct.RiskThirdPartyControlCustomText_RiskId,
            tpc.ThirdPartyControl_Label as CustomLabel,
            COALESCE(
                rtpcct.RiskThirdPartyControlCustomText_TextData,
                CONVERT(
                    CHAR(10),
                    rtpcct.RiskThirdPartyControlCustomText_CustomDateValue,
                    126
                ),
                convert(varchar(20), rtpcct.RiskThirdPartyControlCustomText_NumberValue)
            ) as Value
        from rtpcct
        inner join
            tpc
            on rtpcct.RiskThirdPartyControlCustomText_ThirdPartyControlId
            = tpc.ThirdPartyControl_Id
    ),
    rcad as (
        -- drop down, multi select, dynamic attribute actual values selected
        select
            RiskCustomAttributeData_TenantId,
            RiskCustomAttributeData_RiskId,
            RiskCustomAttributeData_ThirdPartyAttributesId,
            RiskCustomAttributeData_ThirdPartyControlId,
            RiskCustomAttributeData_UserId,
            RiskCustomAttributeData_OrganizationUnitId
        from {{ ref("vwRiskCustomAttributeData") }}
    ),
    tpa as (
        -- drop down, multi select, dynamic attribute list of possible values
        select
            ThirdPartyAttributes_Id,
            ThirdPartyAttributes_Label,
            ThirdPartyAttributes_ThirdPartyControlId
        from {{ ref("vwThirdPartyAttributes") }}
    ),
    rcad_tpa_tpc as (
        select
            rcad.RiskCustomAttributeData_TenantId Risk_TenantId,
            rcad.RiskCustomAttributeData_RiskId,
            tpc.ThirdPartyControl_Label as CustomLabel,
            tpa.ThirdPartyAttributes_Label as Value
        from rcad
        inner join
            tpa
            on rcad.RiskCustomAttributeData_ThirdPartyAttributesId
            = tpa.ThirdPartyAttributes_Id
        inner join
            tpc
            on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
            and tpc.ThirdPartyControl_Label <> 'Type'
    ),
    rcad_au_aou as (
        select
            rcad.RiskCustomAttributeData_TenantId Risk_TenantId,
            rcad.RiskCustomAttributeData_RiskId,
            tpc.ThirdPartyControl_Label as CustomLabel,
            au.AbpUsers_FullName as Value
        from rcad
        inner join
            tpc
            on rcad.RiskCustomAttributeData_ThirdPartyControlId = tpc.ThirdPartyControl_Id
            and tpc.ThirdPartyControl_Label <> 'Type'
        inner join {{ ref("vwAbpUser") }} au
        on au.AbpUsers_Id = rcad.RiskCustomAttributeData_UserId
    union all
            select
            rcad.RiskCustomAttributeData_TenantId Risk_TenantId,
            rcad.RiskCustomAttributeData_RiskId,
            tpc.ThirdPartyControl_Label as CustomLabel,
            aou.AbpOrganizationUnits_DisplayName as Value
        from rcad
        inner join
            tpc
            on rcad.RiskCustomAttributeData_ThirdPartyControlId = tpc.ThirdPartyControl_Id
            and tpc.ThirdPartyControl_Label <> 'Type'
        inner join {{ ref("vwAbpOrganizationUnits") }} aou
        on aou.AbpOrganizationUnits_Id = rcad.RiskCustomAttributeData_OrganizationUnitId
    )

select *
from rtpcct_tpc

union all

select *
from rcad_tpa_tpc

union all

select *
from rcad_au_aou