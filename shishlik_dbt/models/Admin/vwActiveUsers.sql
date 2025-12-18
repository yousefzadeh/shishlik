select distinct
abt.Id TenantId
,au.[Id] UserId
,abt.[Name] TenantName
,au.[Name]+' '+au.[Surname] AbpUsers_UserName
,abt.[CreationTime] AbpTenants_CreationTime
,abt.[CreatorUserId] AbpTenants_CreatorUserId
,abt.[DeleterUserId] AbpTenants_DeleterUserId
,abt.[DeletionTime] AbpTenants_DeletionTime
,abt.EditionId AbpTenants_EditionId
,abt.[LastModificationTime] AbpTenants_LastModificationTime
,abt.[LastModifierUserId] AbpTenants_LastModifierUserId
,abt.[IsSeller] AbpTenants_IsSeller
,abt.[ServiceProviderId] AbpTenants_ServiceProviderId
,abt.[ProductLabel] AbpTenants_ProductLabel
,abt.[HasCompletedProfileSetup] AbpTenants_HasCompletedProfileSetup
,abt.[HasUpgradedToFreeTrial] AbpTenants_HasUpgradedToFreeTrial
,abt.[IndustryId] AbpTenants_IndustryId
,abt.[ThirdPartyLabel] AbpTenants_ThirdPartyLabel
,abt.[LocationId] AbpTenants_LocationId
,abt.[SizeOfTeam] AbpTenants_SizeOfTeam
,abt.[CompanySize] AbpTenants_CompanySize
,abt.[Specialisation] AbpTenants_Specialisation
,abt.[ExternalIdentityProvider] AbpTenants_ExternalIdentityProvider
,abt.[IsTemplate] AbpTenants_IsTemplate
,abt.[IsAvailableForMarketplace] AbpTenants_IsAvailableForMarketplace
,abt.[IsPublishedToMarketplace] AbpTenants_IsPublishedToMarketplace
,abt.[TemplateLastUpdatedTime] AbpTenants_TemplateLastUpdatedTime
,abt.[SSODomains] AbpTenants_SSODomains
,abt.[IsSSOEnabled] AbpTenants_IsSSOEnabled
,abt.[IsUsingGroupsScope] AbpTenants_IsUsingGroupsScope
,abt.[IsInternal] AbpTenants_IsInternal
,au.[AccessFailedCount] AbpUsers_AccessFailedCount
,au.[CreationTime] AbpUsers_CreationTime
,au.[CreatorUserId] AbpUsers_CreatorUserId
,au.[DeleterUserId] AbpUsers_DeleterUserId
,au.[DeletionTime] AbpUsers_DeletionTime
,au.[EmailAddress] AbpUsers_EmailAddress
,au.[IsActive] AbpUsers_IsActive
,au.[IsEmailConfirmed] AbpUsers_IsEmailConfirmed
,au.[IsLockoutEnabled] AbpUsers_IsLockoutEnabled
,au.[LastLoginTime] AbpUsers_LastLoginTime
,au.[LastModificationTime] AbpUsers_LastModificationTime
,au.[LastModifierUserId] AbpUsers_LastModifierUserId
,au.[LockoutEndDateUtc] AbpUsers_LockoutEndDateUtc
,au.[Name] AbpUsers_FirstName
,au.[Surname] AbpUsers_Surname
,au.[IsInvitedAdvisor] AbpUsers_IsInvitedAdvisor
,au.[JobTitle] AbpUsers_JobTitle
,au.[IsHidden] AbpUsers_IsHidden
,au.[LicenseType] AbpUsers_LicenseType
from {{ source("assessment_models", "AbpTenants") }} abt
join {{ source("assessment_models", "AbpUsers") }} au 
on au.TenantId = abt.Id and au.IsDeleted = 0 and au.IsActive = 1
where abt.IsDeleted = 0 and abt.IsActive = 1