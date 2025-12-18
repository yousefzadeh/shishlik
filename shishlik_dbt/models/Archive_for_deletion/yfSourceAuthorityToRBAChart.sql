-- Source Authority RBA chart
select distinct
    TOP 10000
    "Source Authority to RBA"."section_Authority_Field_Name" as C2,
    "Source Authority to RBA"."Authority_Field_Value" as C4,
    "Source Authority to RBA"."Actual_Response" as C6,
    COUNT(DISTINCT("Source Authority to RBA"."Response_Id")) as C7
from {{ ref("vwSourceAuthorityToRBA") }} as "Source Authority to RBA"
where
    ("Source Authority to RBA"."filter_TenantId" in (1384))
    and (
        "Source Authority to RBA"."filter_Source_Authority" = 'ISO/IEC 27001:2013'
        and "Source Authority to RBA"."filter_Template_Name" in ('No Template')
        and "Source Authority to RBA"."filter_Assessment_Name" in ('RBA Provision')
        and "Source Authority to RBA"."filter_Assessment_Field_Name" = 'Field 1'
    )
group by
    "Source Authority to RBA"."Actual_Response",
    "Source Authority to RBA"."Authority_Field_Value",
    "Source Authority to RBA"."section_Authority_Field_Name"
