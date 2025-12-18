-- Target Authority RBA chart
select distinct
    TOP 10000
    "Target Authority To RBA"."section_Authority_Field_Name" as C2,
    "Target Authority To RBA"."Authority_Field_Value" as C4,
    "Target Authority To RBA"."Actual_Response" as C6,
    COUNT(
        DISTINCT("Target Authority To RBA"."Assessment_Name" + '_' + "Target Authority To RBA"."Provision_IDRef")
    ) as C7
from {{ ref("vwTargetAuthorityToRBA") }} as "Target Authority To RBA"
where
    ("Target Authority To RBA"."filter_TenantId" in (1384))
    and (
        "Target Authority To RBA"."filter_Source_Authority" = 'ISO/IEC 27001:2013'
        and "Target Authority To RBA"."filter_Target_Authority" = 'ISO/IEC 27001:2013 Annex A'
        and "Target Authority To RBA"."filter_Template_Name" in ('No Template')
        and "Target Authority To RBA"."filter_Assessment_Name" in ('RBA Provision')
        and "Target Authority To RBA"."filter_Assessment_Field_Name" = 'Field 1'
    )
group by
    "Target Authority To RBA"."section_Authority_Field_Name",
    "Target Authority To RBA"."Authority_Field_Value",
    "Target Authority To RBA"."Actual_Response"
