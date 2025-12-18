select
pc.TenantId,
pc.Id ProvisionControl_Id,
pc.AuthorityReferenceId AuthorityProvision_Id,
pc.ControlsId Controls_Id,
c.Controls_Name Provision_LinkedControls

from {{ source("authority_ref_models", "ProvisionControl") }} pc
join {{ref("vControls")}} c
on c.Controls_Id = pc.ControlsId
and c.TenantId = pc.TenantId
where pc.IsDeleted = 0