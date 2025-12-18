select distinct
    filter_Source_Authority,
    filter_Target_Authority,
    filter_Template_Name,
    filter_Assessment_Name,
    filter_Assessment_Field_Name
from {{ ref("vwTargetAuthorityToRBA") }} tcsatr
{# where filter_TenantId = 1384 #}
