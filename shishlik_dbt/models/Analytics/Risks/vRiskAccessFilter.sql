with uni as (
select distinct
r.TenantEntityUniqueId IdRef,
r.Id Risk_Id,
r.TenantId,
'USERID' as IdentifierType,
au.Id Risk_Access_Id,
au.ReportingPlatformId Identifier,
'Analytics Risk' AS ReferenceType

from {{ source("risk_ref_models", "Risk") }} r
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.TenantId = r.TenantId
join {{ source("abp_ref_models", "AbpUserRoles") }} aur
on aur.UserId = au.Id
join {{ source("abp_ref_models", "AbpRoles") }} ar
on ar.Id = aur.RoleId
left join {{ ref("vRiskAccessMembers") }} ru
on ru.UserId = au.Id
and ru.Risk_Id = r.Id
left join {{ source("abp_ref_models", "AbpPermissions") }} ap
on ap.RoleId = ar.Id and ap.Name = 'General.Risks.ViewAll'
left join {{ source("abp_ref_models", "AbpPermissions") }} app
on app.UserId = au.Id and app.Name = 'General.Risks.ViewAll'

where au.ReportingPlatformId is not null
and case
when app.IsGranted = 1 then au.Id
when app.IsGranted = 0 then ru.UserId
when app.IsGranted is null and ap.IsGranted = 1 then au.Id
when app.IsGranted is null and ap.IsGranted = 0 then ru.UserId
end is not null

union all

select distinct
r.TenantEntityUniqueId IdRef,
r.Id Risk_Id,
r.TenantId,
'USERID' as IdentifierType,
au2.Id Risk_Access_Id,
au2.ReportingPlatformId Identifier,
'Analytics Risk' AS ReferenceType

from {{ source("risk_ref_models", "Risk") }} r
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.TenantId = r.TenantId
join {{ source("abp_ref_models", "AdvisorInvite") }} ai
on ai.TenantId = au.TenantId
and ai.UserIdInTenant = au.Id
join {{ source("abp_ref_models", "AbpUsers") }} au2
on au2.TenantId = ai.ServiceProviderId
and au2.EmailAddress = ai.EmailAddress
join {{ source("abp_ref_models", "AbpUserRoles") }} aur
on aur.UserId = au.Id
join {{ source("abp_ref_models", "AbpRoles") }} ar
on ar.Id = aur.RoleId
left join {{ ref("vRiskAccessMembers") }} ru
on ru.UserId = au.Id
and ru.Risk_Id = r.Id
left join {{ source("abp_ref_models", "AbpPermissions") }} ap
on ap.RoleId = ar.Id and ap.Name = 'General.Risks.ViewAll'
left join {{ source("abp_ref_models", "AbpPermissions") }} app
on app.UserId = au.Id and app.Name = 'General.Risks.ViewAll'

where au.ReportingPlatformId is not null
and case
when app.IsGranted = 1 then au.Id
when app.IsGranted = 0 then ru.UserId
when app.IsGranted is null and ap.IsGranted = 1 then au.Id
when app.IsGranted is null and ap.IsGranted = 0 then ru.UserId
end is not null
)

select
IdRef,
Risk_Id,
TenantId,
IdentifierType,
Risk_Access_Id,
Identifier,
ReferenceType

from uni