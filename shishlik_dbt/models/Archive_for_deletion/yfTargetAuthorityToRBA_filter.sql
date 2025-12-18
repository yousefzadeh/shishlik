select distinct
    "Target Authority To RBA"."filter_TenantId" as C2,
    "Target Authority To RBA"."filter_Source_Authority" as C4,
    "Target Authority To RBA"."filter_Target_Authority" as C6
from {{ ref("vwTargetAuthorityToRBA") }} as "Target Authority To RBA"
where ("Target Authority To RBA"."filter_TenantId" in (3))
