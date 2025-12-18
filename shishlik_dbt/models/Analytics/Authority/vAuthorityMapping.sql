select
tam.TenantId,
tam.CreationTime AuthotityMapping_CreationTime,
tam.SourceAuthorityId Source_Authority_Id,
tam.TargetAuthorityId Target_Authority_Id,
a.Name Target_Authority_Name,
tam.SimilarityPercentage AuthorityMapping_SimilarityPercent

from {{ source("authority_ref_models", "TenantAuthorityMapping") }} tam
join {{ source("authority_ref_models", "Authority") }} a on a.Id = tam.TargetAuthorityId and a.IsDeleted = 0 and a.IsArchived = 0 --and a.AuthoritySector = 'All'
where tam.IsDeleted = 0