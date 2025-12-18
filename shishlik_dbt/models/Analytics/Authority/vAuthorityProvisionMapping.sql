select
tp.TenantId,
tp.CreationTime ProvisionMapping_CreationTime,
tp.SourceAuthorityProvisionId Source_Provision_Id,
tp.TargetAuthorityProvisionId Target_Provision_Id,
ap.ReferenceId Target_Provision_IdRef,
ap.Name Target_Provision_Name,
tp.Similarity*100 Provision_Mapping_SimilarityPercent

from {{ source("authority_ref_models", "TenantAuthorityProvisionMapping") }} tp
join {{ source("authority_ref_models", "AuthorityProvision") }} ap on ap.Id = tp.TargetAuthorityProvisionId and ap.IsDeleted = 0
where tp.IsDeleted = 0