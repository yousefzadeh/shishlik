select
ap.TenantId,
ap.AuthorityId Authority_Id,
cs.ControlSet_Id,
cs.ControlSet_Name Authority_LinkedControlSets

from {{ source("authority_ref_models", "AuthorityPolicy") }} ap
join {{ref("vControlSet")}} cs
on cs.ControlSet_Id = ap.PolicyId
and cs.TenantId = ap.TenantId
where ap.IsDeleted = 0