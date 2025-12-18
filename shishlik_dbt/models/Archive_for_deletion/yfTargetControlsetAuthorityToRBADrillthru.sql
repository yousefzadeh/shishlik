-- Target Controlset Authority RBA Drillthru
select distinct
    TOP 10000
    "Target Control Set Authority To RBA".part,
    "Target Control Set Authority To RBA"."section_Authority_Field_Name"
    + ' : '
    + "Target Control Set Authority To RBA"."Authority_Field_Value" as C1,
    "Target Control Set Authority To RBA"."section_Authority_Field_Name" as C3,
    "Target Control Set Authority To RBA"."Assessment_Name" as C5,
    "Target Control Set Authority To RBA"."AssessmentDomain_Name" as C7,
    "Target Control Set Authority To RBA"."Provision_IDRef" as C9,
    "Target Control Set Authority To RBA"."Provision_Name" as C11,
    "Target Control Set Authority To RBA"."Actual_Response" as C13
from {{ ref("vwTargetControlSetAuthorityToRBA") }} as "Target Control Set Authority To RBA"
where
    (
        "Target Control Set Authority To RBA"."filter_TenantId" = 1384
        and "Target Control Set Authority To RBA"."filter_Source_ControlSet" = 'CS Mar 02'
        and "Target Control Set Authority To RBA"."filter_Target_Authority" = 'ISO/IEC 27001:2013 Annex A'
        and "Target Control Set Authority To RBA"."filter_Template_Name" in ('No Template')
        and "Target Control Set Authority To RBA"."filter_Assessment_Name" in ('RBA Control Mar 02')
    -- AND "Target Control Set Authority To RBA"."filter_Assessment_Field_Name" IN ('Dropdown')
    -- AND "Target Control Set Authority To RBA"."Authority_Field_Value" IN ('To prevent exploitation of technical
    -- vulnerabilities.')
    -- AND "Target Control Set Authority To RBA"."Actual_Response" IN ('False')
    )
