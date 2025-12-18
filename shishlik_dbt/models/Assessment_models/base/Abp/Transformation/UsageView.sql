select distinct
    -- Id,
    at2.AbpTenants_Id TenantId,
    ae.AbpEditions_DisplayName EditionName,
    at2.AbpTenants_Name TenantName,
    -- at2.AbpTenants_CreationTime,
    au.AbpUsers_FullName UserName,
    aula.AbpUserLoginAttempts_UserNameOrEmailAddress,
    aula.AbpUserLoginAttempts_Result,
    -- aula.AbpUserLoginAttempts_CreationTime LoginAttemptCreationTime,
    case when aula.AbpUserLoginAttempts_Result = 1 then min(aula.AbpUserLoginAttempts_CreationTime) end First_Login,
    au.AbpUsers_LastLoginTime Last_Login,
    cast(Format(au.AbpUsers_LastLoginTime, 'MMM, yyyy') as varchar) LastLoginMonth,
    cast(
        datediff(
            month,
            case when aula.AbpUserLoginAttempts_Result = 1 then min(aula.AbpUserLoginAttempts_CreationTime) end,
            au.AbpUsers_LastLoginTime
        ) as varchar
    )
    + ' Months' ActiveTime,
    COUNT(case when aula.AbpUserLoginAttempts_Result = 1 then 1 end) NumberofLogins
from {{ ref("vwAbpTenants") }} at2
left join {{ ref("vwAbpUserLoginAttempts") }} aula on aula.AbpUserLoginAttempts_TenantId = at2.AbpTenants_Id
left join {{ ref("vwAbpUser") }} au on au.AbpUsers_Id = aula.AbpUserLoginAttempts_UserId
left join {{ ref("vwAbpEditions") }} ae on ae.AbpEditions_Id = at2.AbpTenants_EditionId

group by
    at2.AbpTenants_Id,
    ae.AbpEditions_DisplayName,
    at2.AbpTenants_Name,
    au.AbpUsers_FullName,
    aula.AbpUserLoginAttempts_UserNameOrEmailAddress,
    aula.AbpUserLoginAttempts_Result,
    -- aula.AbpUserLoginAttempts_CreationTime,
    au.AbpUsers_LastLoginTime
