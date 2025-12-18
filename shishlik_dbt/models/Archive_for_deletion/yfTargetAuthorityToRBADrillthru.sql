-- Target Authority RBA drill thru all rows for an assessment
select distinct
    TOP 10000
    "Target Authority To RBA".part,
    "Target Authority To RBA"."section_Authority_Field_Name"
    + ' : '
    + "Target Authority To RBA"."Authority_Field_Value" as C1,
    "Target Authority To RBA"."filter_Assessment_Field_Name" as C3,
    "Target Authority To RBA"."Assessment_Name" as C5,
    "Target Authority To RBA"."AssessmentDomain_Name" as C7,
    "Target Authority To RBA"."Provision_IDRef" as C9,
    "Target Authority To RBA"."Provision_Name" as C11,
    "Target Authority To RBA"."Actual_Response" as C13
from {{ ref("vwTargetAuthorityToRBA") }} as "Target Authority To RBA"
where
    (
        "Target Authority To RBA"."filter_TenantId" = 1384
        and "Target Authority To RBA"."filter_Source_Authority" in ('ISO/IEC 27001:2013')
        and "Target Authority To RBA"."filter_Target_Authority" in ('ISO/IEC 27001:2013 Annex A')
        and "Target Authority To RBA"."filter_Template_Name" in ('No Template')
        and "Target Authority To RBA"."filter_Assessment_Name" in ('RBA Provision')
    -- AND "Target Authority To RBA"."filter_Assessment_Field_Name" IN ('Field 1')
    -- AND "Target Authority To RBA"."Authority_Field_Value" IN ('To ensure that information security is designed and
    -- implemented within the development lifecycle of information systems.')
    -- AND "Target Authority To RBA"."Actual_Response" IN ('No Response')
    )
