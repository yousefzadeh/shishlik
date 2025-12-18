select
Tenant_Id,
Tenant_Name,
cast(EntityId as bigint) EntityId,
Id,
Date_Time,
Event,
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vRoleCreated") }}

union all

select
Tenant_Id,
Tenant_Name,
cast(EntityId as bigint) EntityId,
Id,
Date_Time,
Event,
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vRoleDeleted") }}

union all

select
Tenant_Id,
Tenant_Name,
cast(EntityId as bigint) EntityId,
Id,
Date_Time,
Event,
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vRoleEditedPermissionAdded") }}

union all

select
Tenant_Id,
Tenant_Name,
cast(EntityId as bigint) EntityId,
Id,
Date_Time,
Event,
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vRoleEditedPermissionRemoved") }}