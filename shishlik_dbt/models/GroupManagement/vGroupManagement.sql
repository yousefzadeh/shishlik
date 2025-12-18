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

from {{ ref("vGroupCreated") }}

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

from {{ ref("vGroupDeleted") }}

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

from {{ ref("vGroupEditedUserAdded") }}

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

from {{ ref("vGroupEditedUserRemoved") }}

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

from {{ ref("vGroupNameEdited") }}