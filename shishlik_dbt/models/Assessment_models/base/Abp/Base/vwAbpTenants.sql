{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [ConnectionString],
            [CreationTime],
            [CreatorUserId],
            [CustomCssId],
            [DeleterUserId],
            [DeletionTime],
            [EditionId],
            [IsActive],
            [IsDeleted],
            cast(coalesce(LastModificationTime,CreationTime) as datetime2) as [LastModificationTime],
            [LastModifierUserId],
            [LogoFileType],
            [LogoId],
            [Name],
            [TenancyName],
            [IsSeller],
            [ImportFileLogId],
            cast([ReferralCode] as nvarchar(4000)) ReferralCode,
            [ServiceProviderId],
            cast([ProductLabel] as nvarchar(4000)) ProductLabel,
            [HasCompletedProfileSetup],
            [SignedUpUsingReferralUrl],
            cast([ExternalCustomerId] as nvarchar(4000)) ExternalCustomerId,
            [HasUpgradedToFreeTrial],
            [IndustryId],
            [SignedUpAppType],
            cast([ThirdPartyLabel] as nvarchar(4000)) ThirdPartyLabel,
            [LocationId],
            [SizeOfTeam],
            [CompanySize],
            cast([Specialisation] as nvarchar(4000)) Specialisation,
            cast([ExternalAuthorizationServerIssuerUrl] as nvarchar(4000)) ExternalAuthorizationServerIssuerUrl,
            cast([SinglePageApplicationClientId] as nvarchar(4000)) SinglePageApplicationClientId,
            [CreatedUsingSPDashboard],
            [IsTemplate],
            [IsConfiguredForStripeGateway],
            [IsAvailableForMarketplace],
            [IsPublishedToMarketplace],
            [IsInternal],
            COUNT(Id) over (partition by ServiceProviderId) Total6ClicksCustomers
        from {{ source("assessment_models", "AbpTenants") }} {{ system_remove_IsDeleted() }}
    ),
    edition as (
        select 
            Id as Edition_Id,
            Name as Edition_Name, 
            IsServiceProviderEdition, 
            cast(coalesce(LastModificationTime,CreationTime) as datetime2) as UpdateTime
        from {{ source("assessment_models", "AbpEditions") }}
    )
select
    {{ col_rename("Id", "AbpTenants") }},
    {{ col_rename("ConnectionString", "AbpTenants") }},
    {{ col_rename("CreationTime", "AbpTenants") }},
    {{ col_rename("CreatorUserId", "AbpTenants") }},

    {{ col_rename("CustomCssId", "AbpTenants") }},
    {{ col_rename("DeleterUserId", "AbpTenants") }},
    {{ col_rename("DeletionTime", "AbpTenants") }},
    {{ col_rename("EditionId", "AbpTenants") }},

    {{ col_rename("IsActive", "AbpTenants") }},
    {{ col_rename("IsDeleted", "AbpTenants") }},
    {{ col_rename("LastModificationTime", "AbpTenants") }},
    {{ col_rename("LastModifierUserId", "AbpTenants") }},

    {{ col_rename("LogoFileType", "AbpTenants") }},
    {{ col_rename("LogoId", "AbpTenants") }},
    {{ col_rename("Name", "AbpTenants") }},
    {{ col_rename("TenancyName", "AbpTenants") }},

    {{ col_rename("IsSeller", "AbpTenants") }},
    {{ col_rename("ImportFileLogId", "AbpTenants") }},
    {{ col_rename("ReferralCode", "AbpTenants") }},
    {{ col_rename("ServiceProviderId", "AbpTenants") }},

    {{ col_rename("ProductLabel", "AbpTenants") }},
    {{ col_rename("HasCompletedProfileSetup", "AbpTenants") }},
    {{ col_rename("SignedUpUsingReferralUrl", "AbpTenants") }},
    {{ col_rename("ExternalCustomerId", "AbpTenants") }},

    {{ col_rename("HasUpgradedToFreeTrial", "AbpTenants") }},
    {{ col_rename("IndustryId", "AbpTenants") }},
    {{ col_rename("SignedUpAppType", "AbpTenants") }},
    {{ col_rename("ThirdPartyLabel", "AbpTenants") }},

    {{ col_rename("LocationId", "AbpTenants") }},
    {{ col_rename("SizeOfTeam", "AbpTenants") }},
    {{ col_rename("CompanySize", "AbpTenants") }},
    {{ col_rename("Specialisation", "AbpTenants") }},

    {{ col_rename("ExternalAuthorizationServerIssuerUrl", "AbpTenants") }},
    {{ col_rename("SinglePageApplicationClientId", "AbpTenants") }},

    {{ col_rename("CreatedUsingSPDashboard", "AbpTenants") }},

    {{ col_rename("IsTemplate", "AbpTenants") }},
    {{ col_rename("IsConfiguredForStripeGateway", "AbpTenants") }},
    {{ col_rename("IsAvailableForMarketplace", "AbpTenants") }},

    {{ col_rename("IsPublishedToMarketplace", "AbpTenants") }},
    {{ col_rename("IsInternal", "AbpTenants") }},
    e.IsServiceProviderEdition as AbpTenants_IsHubAndSpoke,
    Total6ClicksCustomers as AbpTenants_Total6ClicksCustomers,
     GREATEST(cast(base.LastModificationTime as datetime2), cast(e.UpdateTime as datetime2)) AbpTenants_UpdateTime
from base
join edition e on e.Edition_Id = base.EditionId
where base.IsActive = 1 and base.IsDeleted = 0 and e.Edition_Name <> 'NoFeature'
