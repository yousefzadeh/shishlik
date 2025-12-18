select
es.TenantId Source_TenantId,
es.SourceEntityId Source_EntityId,
es.CreationTime Transferred_Date,
au.Name+' '+au.Surname Transferred_By,
ed.DestinationTenantId  Destination_TenantId,
t.Name Destination_TenantName,
ed.DestinationEntityId  Destination_EntityId

from {{ source("miscellaneous_ref_models", "EntityTransferLogSource") }} es
join {{ source("miscellaneous_ref_models", "EntityTransferLogDestination") }} ed
on ed.TenantId = es.TenantId
and es.EntityTransferLogId = ed.EntityTransferLogSourceId
and ed.IsDeleted = 0
join {{ source("abp_ref_models", "AbpTenants") }} t
on t.Id = ed.DestinationTenantId
and t.IsDeleted = 0 and t.IsActive = 1
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = es.CreatorUserId
and au.TenantId = es.TenantId
and au.IsDeleted = 0 and au.IsActive = 1

where es.IsDeleted = 0