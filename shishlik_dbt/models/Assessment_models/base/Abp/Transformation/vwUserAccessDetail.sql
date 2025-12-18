{#
    Details of How a User has access to a Tenant

    Users:
    A unique User Person is defined by a unique normalized email address.
    A UserId is assigned for each User Person with access to a Tenant in AbpUsers table.
    A UserId that has access to Yellowfin SSO has a ReportingPlatformId that is assigned on first login to Yellowfin SSO but not at Yellowfin Standalone.

    A paying customer account has many types of subscriptions defined in AbpEditions table.
    The types of subscriptions are divided into 2 groups:
    1. Standalone subscription plan
    2. Hub and Spoke Subscription plan

    In a Hub and Spoke subscription plan:
    1. Relationship of hub to spoke is defined in TenantVendor table
    2. Hub Tenant has AbpEditions.IsServiceProviderEdition = 1 in join on AbpTenants to AbpEditions
    3. Hub Tenant has Spoke Tenants (VendorID) as defined in TenantVendor table (TenantID 1 to many VendorId)

    In a Standalone subscription plan
    1. Standalone Tenant has AbpEditions.IsServiceProviderEdition = 0 in join on AbpTenants to AbpEditions
    2. Standalone Tenant has ThirdParty (Responding Team or Team) relationships defined in TenantVendor (VendorID)  
    3. Users in Standalone Tenant can only view its own Tenancy 
    
    Permissions for users of Hub Tenant to access data in Spoke Tenant:
    1. User at Hub is invited by User at Client Tenant - relationship defined in AdvisorInvite table
    2. Regardless of the relationship of Hub to Spoke, User at Hub can only view data at Spoke only if Invited on per user basis

#}
with
    all_user as (
        -- All users of a Tenant can access data of their tenancy
        select
            u.ReportingPlatformId,
            case when u.ReportingPlatformId is NULL then 0 else 1 end HasYellowfinSSO,
            u.Id,
            u.TenantId,
            u.NormalizedEmailAddress NormalEmail,
            t.AbpTenants_IsHubAndSpoke,
            u.IsDeleted,
            u.IsEmailConfirmed,
            u.[Name] + ' ' + u.[Surname] FullName
        from {{ source("assessment_models", "AbpUsers") }} u
        join {{ ref("vwAbpTenants") }} t on u.TenantId = t.AbpTenants_id
    ),
    hub_spoke AS (
        -- Relation of Hub to Spoke Tenants
        SELECT
            tv.TenantId Hub_TenantId,
            tv.VendorId Spoke_TenantId
        FROM {{ source('tenant_models', 'TenantVendor') }} tv
        JOIN {{ ref("vwAbpTenants") }} hub
            ON tv.TenantId = hub.AbpTenants_Id
        JOIN {{ ref("vwAbpTenants") }} spoke
            ON tv.VendorId = spoke.AbpTenants_Id
        WHERE tv.isarchived = 0 -- Only want not archived
            AND hub.AbpTenants_IsHubAndSpoke = 1 -- Only Hub tenants
    ), 
    standalone_user as (
        -- Users of Standalone Tenants  
        select * 
        from all_user 
        left join hub_spoke 
            on all_user.TenantId = hub_spoke.Spoke_TenantId
        where AbpTenants_IsHubAndSpoke = 0
        and hub_spoke.Spoke_TenantId is null
    ),
    spoke_user as (
        -- Users of Spoke Tenants  
        select all_user.* 
        from all_user 
        left join hub_spoke 
            on all_user.TenantId = hub_spoke.Spoke_TenantId
        where AbpTenants_IsHubAndSpoke = 0
        and hub_spoke.Spoke_TenantId is not null
    ),
    hub_user_invited_by_spoke_user as (
        -- - User at hub invite by user at spoke, gets a NEW UserId at Spoke
        select
            ai.TenantId Spoke_TenantId,
            hub_user.Id Hub_UserId,
            ai.ServiceProviderId Hub_TenantId,
            -- Invited hub user gets a new userid in spoke
            ai.UserIdInTenant Spoke_UserId
        from {{ source("assessment_models", "AdvisorInvite") }} ai
        join
            all_user hub_user
            on ai.ServiceProviderId = hub_user.TenantId
            and upper(ai.EmailAddress) = hub_user.NormalEmail
        join {{ ref("vwAbpTenants") }} t on ai.TenantId = t.AbpTenants_id
        -- must not be a template and 'HasAcceptedInvite' is true
        where t.AbpTenants_IsTemplate = 0 and ai.HasAcceptedInvite = 1
    ),
    allowed_by_invite as (
        select Spoke_TenantId Tenant_Id, Hub_UserId, 'Invited User' Reason from hub_user_invited_by_spoke_user
    ),
    hub_user_read_spoke as (
        select u.ReportingPlatformId, u.HasYellowfinSSO, u.NormalEmail, pi.Hub_UserId User_Id, pi.Tenant_Id, pi.Reason, u.FullName
        from allowed_by_invite pi
        join all_user u on pi.Hub_UserId = u.Id
    ),
    hub_only_user as (
        -- Users of Hub Tenants have access only to Hub data  
        select all_user.* 
        from all_user 
        left join hub_spoke 
            on all_user.TenantId = hub_spoke.Hub_TenantId
        left join hub_user_read_spoke on all_user.Id = hub_user_read_spoke.User_Id
        where AbpTenants_IsHubAndSpoke = 1
        and hub_spoke.Hub_TenantId is not null
        and hub_user_read_spoke.User_Id is null
    ),    
    uni as (
        -- NormalEmail - this indicates the unique User Person
        -- HasYellowfinSSO - this indicates the user has access to Yellowfin SSO
        -- Reason - Reason that UserId has access to Tenant
        -- Users at Hub that are allowed to see data of Spoke
        select 'Hub User (access to Spoke Data)' UserType, ReportingPlatformId, HasYellowfinSSO, NormalEmail, User_Id, Tenant_Id, Reason, FullName
        from hub_user_read_spoke

        union all

        -- Users at any Tenant are allowed to see Tenant Data
        select 'User of Stand-alone Tenant' UserType, ReportingPlatformId, HasYellowfinSSO, NormalEmail, Id User_Id, TenantId Tenant_Id, 'Tenant user' Reason, FullName
        from standalone_user

        union all 

        -- Users at any Tenant are allowed to see Tenant Data
        select 'Hub User (does not have access to Spoke Data)' UserType, ReportingPlatformId, HasYellowfinSSO, NormalEmail, Id User_Id, TenantId Tenant_Id, 'Tenant user' Reason, FullName
        from hub_only_user
        union all 

        -- Users at any Tenant are allowed to see Tenant Data
        select 'Spoke User' UserType, ReportingPlatformId, HasYellowfinSSO, NormalEmail, Id User_Id, TenantId Tenant_Id, 'Tenant user' Reason, FullName
        from spoke_user
    )
select UserType, ReportingPlatformId, User_Id, Tenant_Id, Reason, FullName
from uni
