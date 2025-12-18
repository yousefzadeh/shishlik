-- Source Authority RBA drill thru all rows for an assessment
select distinct
    TOP 10000
    "Source Authority to RBA".part,
    "Source Authority to RBA"."section_Authority_Field_Name"
    + ' : '
    + "Source Authority to RBA"."Authority_Field_Value" as C1,
    "Source Authority to RBA"."Assessment_Name" as C3,
    "Source Authority to RBA"."Provision_IDRef" as C5,
    "Source Authority to RBA"."Provision_Name" as C7,
    "Source Authority to RBA"."Actual_Response" as C9,
    "Source Authority to RBA"."Response_Id" as C11,
    "Source Authority to RBA"."AuthorityProvision_Id" as C13
from {{ ref("vwSourceAuthorityToRBA") }} as "Source Authority to RBA"
where
    (
        "Source Authority to RBA"."filter_TenantId" in (1384)
        and "Source Authority to RBA"."filter_Source_Authority" in ('ISO/IEC 27001:2013')
        and "Source Authority to RBA"."filter_Template_Name" in ('No Template')
    -- AND "Source Authority to RBA"."filter_Assessment_Name" IN ('RBA Provision')
    -- AND "Source Authority to RBA"."Authority_Field_Value" IN ('8 Operation')
    -- AND "Source Authority to RBA"."Actual_Response" IN ('No Response')
    )
