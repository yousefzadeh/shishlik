-- Source Controlset RBA Chart
select distinct
    TOP 10000
    "Source Control Set to RBA"."ControlsDomain_Name" as C2,
    "Source Control Set to RBA"."Actual_Response" as C4,
    COUNT(DISTINCT("Source Control Set to RBA"."Response_Id")) as C5
from {{ ref("vwSourceControlSetToRBA") }} as "Source Control Set to RBA"
where
    ("Source Control Set to RBA"."filter_TenantId" in (1384))
    and (
        "Source Control Set to RBA"."filter_Source_ControlSet" = 'CS Mar 02'
        and "Source Control Set to RBA"."filter_Template_Name" in ('No Template')
        and "Source Control Set to RBA"."filter_Assessment_Name" in ('RBA Control Mar 02')
        and "Source Control Set to RBA"."filter_Assessment_Field_Name" = 'Dropdown'
    )
group by "Source Control Set to RBA"."ControlsDomain_Name", "Source Control Set to RBA"."Actual_Response"
