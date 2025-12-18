select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserActive") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserNameChanged") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserEmailChanged") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserAddedtoGroup") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserAdvisorInvite") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserAdvisorRevoked") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserDeleted") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserInactive") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserInvite") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserLocked") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserLogin") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserPasswordReset") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserRemovedfromGroup") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserUnlocked") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserPermission") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vAdvisorPermission") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserRole") }}

union all

select
Tenant_Id,
Tenant_Name,
EntityId,
Id,
Date_Time,
[Event],
Impacted,
Impacted_IsDeleted,
Changed,
Actioned_By,
Actioned_By_IsDeleted

from {{ ref("vUserImpersonator") }}