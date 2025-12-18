-- union all
select *
from {{ ref("vwAdvisorInviteRevoke") }}

--union all

--select *
--from {{ ref("vwUserImpersonator") }}

union all

select *
from {{ ref("vwUserActiveInactive") }}

union all

select *
from {{ ref("vwUserAddedRemovedfromGroup") }}

union all

select *
from {{ ref("vwUserLockedUnlocked") }}

union all

select *
from {{ ref("vwUserLogin") }}

union all

select *
from {{ ref("vwUserPasswordReset") }}

union all

select *
from {{ ref("vwUserRoleAdded") }}

union all

select *
from {{ ref("vwUserPermissionAddedRemoved") }}

union all

-- User Invite, User Deleted-->done
select
    at2.Id Tenant_Id,
    case when au.CreatorUserId is not null then au.CreationTime else null end Date_Time,
    'User Management' EventType,
    case when au.CreatorUserId is not null then 'User Invite' else null end Event,
    case when au.CreatorUserId is not null then au2.Name + ' ' + au2.Surname else null end Actioned_by,
    case when au.CreatorUserId is not null then au.UserName + ' was invited' else null end Changed,
    case when au.CreatorUserId is not null then au.UserName else null end Impacted
from {{ source("assessment_models", "AbpTenants") }} at2
join {{ source("assessment_models", "AbpUsers") }} au on au.TenantId = at2.Id
left join {{ source("assessment_models", "AbpUsers") }} au2 on au.CreatorUserId = au2.Id and au.TenantId = au2.TenantId
left join {{ source("assessment_models", "AbpUsers") }} au3 on au.DeleterUserId = au3.Id and au.TenantId = au3.TenantId
left join {{ source("assessment_models", "AbpUserRoles") }} aur on aur.UserId = au.Id
left join {{ source("assessment_models", "AbpRoles") }} ar on ar.Id = aur.RoleId
where
    -- at2.Id = 1056
    -- and 
    case when au.CreatorUserId is not null then au.UserName + ' was invited' else null end is not null

union all

-- User Invite, User Deleted-->done
select
    at2.Id Tenant_Id,
    case when au.DeleterUserId is not null and au.IsDeleted = 1 then au.DeletionTime else null end Date_Time,
    'User Management' EventType,
    case when au.DeleterUserId is not null and au.IsDeleted = 1 then 'User Deleted' else null end Event,
    case
        when au.DeleterUserId is not null and au.IsDeleted = 1 then au3.Name + ' ' + au3.Surname else null
    end Actioned_by,
    case when au.DeleterUserId is not null and au.IsDeleted = 1 then au.UserName + ' was deleted' else null end Changed,
    case when au.DeleterUserId is not null and au.IsDeleted = 1 then au.UserName else null end Impacted
from {{ source("assessment_models", "AbpTenants") }} at2
join {{ source("assessment_models", "AbpUsers") }} au on au.TenantId = at2.Id
left join {{ source("assessment_models", "AbpUsers") }} au2 on au.CreatorUserId = au2.Id and au.TenantId = au2.TenantId
left join {{ source("assessment_models", "AbpUsers") }} au3 on au.DeleterUserId = au3.Id and au.TenantId = au3.TenantId
left join {{ source("assessment_models", "AbpUserRoles") }} aur on aur.UserId = au.Id
left join {{ source("assessment_models", "AbpRoles") }} ar on ar.Id = aur.RoleId
where
    -- at2.Id = 1056
    -- and 
    case when au.DeleterUserId is not null and au.IsDeleted = 1 then au.UserName + ' was deleted' else null end
    is not null
