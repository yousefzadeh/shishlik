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
			t.AbpTenants_UpdateTime 
        from {{ source("assessment_models", "AbpUsers") }} u
        join {{ ref("vwAbpTenants") }} t on u.TenantId = t.AbpTenants_id
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
        select Distinct
            ReportingPlatformId
            , User_Id
            , Tenant_Id
            , max(Updatetime)over(partition by ReportingPlatformId, User_Id, Tenant_Id) as ReportUserAccess_Updatetime-- Max of updatetime to extra records wrt multiple timestamps
            , rank()over(order by User_Id, Tenant_Id) as ReportUserAccess_pk--Pk to fix the duplication issue
        from 
        (
            select
            au.ReportingPlatformId,
            case when au.ReportingPlatformId is NULL then 0 else 1 end HasYellowfinSSO,
            au.NormalizedEmailAddress NormalEmail,
            au.Id User_Id,
            ai.AdvisorInvite_TenantId Tenant_Id,
            'Hub user' Reason,
            case when AdvisorInvite_ExpireTimeUtc <= AdvisorInvite_VerifiedOn then AdvisorInvite_VerifiedOn
                else AdvisorInvite_ExpireTimeUtc 
                end as Updatetime
            from {{ ref("vwAdvisorInvite") }} ai
            join {{ source("assessment_models", "AbpUsers") }} au
            on au.TenantId = ai.AdvisorInvite_ServiceProviderId
            and au.EmailAddress = ai.AdvisorInvite_EmailAddress

            union all

            -- Users at any Tenant are allowed to see Tenant Data
            select ReportingPlatformId, HasYellowfinSSO, NormalEmail, Id User_Id, TenantId Tenant_Id, 'Tenant user' Reason, 
                AbpTenants_UpdateTime
            from standalone_user
        )a
    )

select ReportingPlatformId
    , User_Id
    , Tenant_Id
    , ReportUserAccess_Updatetime
    , ReportUserAccess_pk
from uni
