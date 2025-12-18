{{ config(materialized="view") }}

-- The list of possible attribute values for dropdown, multiselect dropdown, and dynamic attributes are stored in the
-- ThirdPartyAttributes table. This table is also used for other entity types.
-- The actual attribute values for issues are stored in two tables:
-- * For dropdown, multiselect dropdown, and dynamic attributes, the values are stored in the IssueCustomAttributeData
-- table.
-- * For free-text, rich-text, and date attributes, the values are stored in the IssueFreeTextControlData table. For
-- free-text and rich-text attributes, the value is stored in the TextData column. For date attributes, the value is
-- stored in the CustomDateValue.
-- The IssueCustomAttributeData and IssueFreeTextControlData tables are only used for issue custom attributes. Other
-- entity types (e.g. risks, third-parties) have their own corresponding tables.
with
    iftcd as (
        -- Attribute values for free text
        select
            IssueFreeTextControlData_IssueId,
            IssueFreeTextControlData_TextData,
            IssueFreeTextControlData_CustomDateValue,
            IssueFreeTextControlData_NumberValue,
            IssueFreeTextControlData_ThirdPartyControlId
        from {{ ref("vwIssueFreeTextControlData") }}

    ),
    tpc as (
        -- Attributes by entity
        select ThirdPartyControl_Id, ThirdPartyControl_Label from {{ ref("vwThirdPartyControl") }}
    ),
    iftcd_tpc as (
        -- Risk_Id, Attribute label, attribute value - free text or date value
        select
            iftcd.IssueFreeTextControlData_IssueId,
            tpc.ThirdPartyControl_Label as CustomLabel,
            COALESCE(
                iftcd.IssueFreeTextControlData_TextData,
                cast(iftcd.IssueFreeTextControlData_CustomDateValue as varchar(30)),
                iftcd.IssueFreeTextControlData_NumberValue
            ) as Value
        from iftcd
        inner join tpc on iftcd.IssueFreeTextControlData_ThirdPartyControlId = tpc.ThirdPartyControl_Id
    ),
    icad as (
        -- drop down, multi select, dynamic attribute actual values selected
        select IssueCustomAttributeData_IssueId, IssueCustomAttributeData_ThirdPartyAttributesId
        from {{ ref("vwIssueCustomAttributeData") }}
    ),
    tpa as (
        -- drop down, multi select, dynamic attribute list of possible values
        select ThirdPartyAttributes_Id, ThirdPartyAttributes_Label, ThirdPartyAttributes_ThirdPartyControlId
        from {{ ref("vwThirdPartyAttributes") }}
    ),
    icad_tpa_tpc as (
        select
            icad.IssueCustomAttributeData_IssueId,
            tpc.ThirdPartyControl_Label as CustomLabel,
            tpa.ThirdPartyAttributes_Label as Value
        from icad
        inner join tpa on icad.IssueCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
        inner join
            tpc
            on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
            and tpc.ThirdPartyControl_Label <> 'Type'
    )

select *
from iftcd_tpc

union all

select *
from icad_tpa_tpc
