select 
ap.Uuid,
ap.AuthorityId Authority_Id,
ap.Id AuthorityProvision_Id,
ap.CreationTime AuthorityProvision_CreationTime,
ap.CreatorUserId AuthorityProvision_CreatorUserId,
ap.LastModificationTime AuthorityProvision_LastModificationTime,
ap.LastModifierUserId AuthorityProvision_LastModifierUserId,
ap.ReferenceId AuthorityProvision_IdRef,
ap.[Name] AuthorityProvision_Name,
ap.[Order] AuthorityProvision_Order
from {{ source("authority_ref_models", "AuthorityProvision") }} ap
where ap.IsDeleted = 0