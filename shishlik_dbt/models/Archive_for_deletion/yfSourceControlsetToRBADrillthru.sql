-- Source Controlset RBA Drillthru   
select distinct
    TOP 10000
    "Source Control Set to RBA".part,
    "Source Control Set to RBA"."Assessment_Name" as C2,
    "Source Control Set to RBA"."ControlsDomain_Name" as C4,
    "Source Control Set to RBA"."Controls_IDRef" as C6,
    "Source Control Set to RBA"."Controls_Name" as C8,
    "Source Control Set to RBA"."Actual_Response" as C10
from {{ ref("vwSourceControlSetToRBA") }} as "Source Control Set to RBA"
where
    (
        "Source Control Set to RBA"."filter_TenantId" = 1384
        and "Source Control Set to RBA"."filter_Source_ControlSet" in ('CS Mar 02')
        and "Source Control Set to RBA"."filter_Template_Name" in ('No Template')
        and "Source Control Set to RBA"."filter_Assessment_Name" in ('RBA Control Mar 02')
    -- AND "Source Control Set to RBA"."filter_Assessment_Field_Name" IN ('Dropdown')
    -- AND "Source Control Set to RBA"."ControlsDomain_Name" IN ('Default')
    -- AND "Source Control Set to RBA"."Actual_Response" IN ('True')
    )
