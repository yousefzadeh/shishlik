
with
    all_user as (
        -- All users of a Tenant can access data of their tenancy
        select
            u.ReportingPlatformId,
            case when u.ReportingPlatformId is NULL then 0 else 1 end HasYellowfinSSO,
            u.Id,
            u.TenantId,
            u.NormalizedEmailAddress NormalEmail,
            e.IsServiceProviderEdition AbpTenants_IsHubAndSpoke
        from {{ source("abp_ref_models", "AbpUsers") }} u
        join {{ source("abp_ref_models", "AbpTenants") }} t on u.TenantId = t.Id
		join {{ source("abp_ref_models", "AbpEditions") }} e on e.Id = t.EditionId
		where t.IsDeleted = 0 and t.IsActive = 1 and e.[Name] <> 'NoFeature'
    ),
    standalone_user as (
        -- Users of Standalone or Spoke Tenants  
        select * from all_user where AbpTenants_IsHubAndSpoke = 0
    ),
    uni as (
        -- NormalEmail - this indicates the unique User Person
        -- HasYellowfinSSO - this indicates the user has access to Yellowfin SSO
        -- Reason - Reason that UserId has access to Tenant
        -- Users at Hub that are allowed to see data of Spoke
        select
        au.ReportingPlatformId,
        case when au.ReportingPlatformId is NULL then 0 else 1 end HasYellowfinSSO,
        au.NormalizedEmailAddress NormalEmail,
        au.Id User_Id,
        ai.TenantId Tenant_Id,
        'Hub user' Reason

        from {{ source("abp_ref_models", "AdvisorInvite") }} ai
        join {{ source("abp_ref_models", "AbpUsers") }} au
        on au.TenantId = ai.ServiceProviderId
        and au.EmailAddress = ai.EmailAddress
		where ai.IsDeleted = 0

        union all

        -- Users at any Tenant are allowed to see Tenant Data
        select ReportingPlatformId, HasYellowfinSSO, NormalEmail, Id User_Id, TenantId Tenant_Id, 'Tenant user' Reason
        from standalone_user
    )

select ReportingPlatformId, User_Id, Tenant_Id, cast(Tenant_Id as varchar(12))+'_'+cast(User_Id as varchar(12)) as TenantUserId
from uni