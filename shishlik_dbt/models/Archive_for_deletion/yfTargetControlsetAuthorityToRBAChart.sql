-- Target Controlset Authority RBA Chart
select distinct
    TOP 10000
    "Target Control Set Authority To RBA"."section_Authority_Field_Name" as C2,
    "Target Control Set Authority To RBA"."Authority_Field_Value" as C4,
    "Target Control Set Authority To RBA"."Actual_Response" as C6,
    COUNT(
        DISTINCT(
            "Target Control Set Authority To RBA"."Assessment_Name"
            + '_'
            + "Target Control Set Authority To RBA"."Provision_IDRef"
        )
    ) as C7
from {{ ref("vwTargetControlSetAuthorityToRBA") }} as "Target Control Set Authority To RBA"
where
    ("Target Control Set Authority To RBA"."filter_TenantId" in (1384))
    and (
        "Target Control Set Authority To RBA"."filter_Source_ControlSet" = 'CS Mar 02'
        and "Target Control Set Authority To RBA"."filter_Target_Authority" = 'ISO/IEC 27001:2013 Annex A'
        and "Target Control Set Authority To RBA"."filter_Template_Name" in ('No Template')
        and "Target Control Set Authority To RBA"."filter_Assessment_Name" in ('RBA Control Mar 02')
        and "Target Control Set Authority To RBA"."filter_Assessment_Field_Name" = 'Dropdown'
    )
group by
    "Target Control Set Authority To RBA"."Actual_Response",
    "Target Control Set Authority To RBA"."Authority_Field_Value",
    "Target Control Set Authority To RBA"."section_Authority_Field_Name"
