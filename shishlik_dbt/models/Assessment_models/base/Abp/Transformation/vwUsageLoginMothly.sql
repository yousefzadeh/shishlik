with
    base1 as (
        select distinct
            at2.Id Tenant_Id,
            at2.Name TenantName,
            ae.DisplayName Edition,
            DATEADD(mi, DATEDIFF(mi, 0, aula.CreationTime), 0) Date_Time,
            au.Name + ' ' + au.Surname UserName,
            au.Id UserId,
            au.CreationTime UserCreationDate,
            au.EmailAddress,
            ROW_NUMBER() over (
                partition by aula.UserId order by aula.TenantId, aula.UserId, aula.CreationTime, aula.Id
            ) FirstLoginFlag,
            au.LastLoginTime

        from {{ source("assessment_models", "AbpUserLoginAttempts") }} aula
        left join {{ source("assessment_models", "AbpTenants") }} at2 on at2.Id = aula.TenantId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = aula.UserId
        left join {{ source("assessment_models", "AbpEditions") }} ae on ae.Id = at2.EditionId

        where aula. [Result] = 1 and at2.Id is not null
        and at2.IsInternal = 0
    ),
    base2 as (
        select distinct *, case when FirstLoginFlag = 1 then Date_Time end FirstLogin

        from base1
        where case when FirstLoginFlag = 1 then Date_Time end is not null
    )

select distinct
    'Logged' Logintype,
    base1.Tenant_Id,
    DATEADD(MONTH, DATEDIFF(MONTH, 0, base1.Date_Time), 0) GenDate,
    base1.Date_Time,
    cast(Format(base1.Date_Time, 'MMM, yyyy') as varchar) LoginMonth,
    year(base1.Date_Time) Year,
    month(base1.Date_Time) Month,
    base1.TenantName,
    base1.Edition,
    base1.UserId,
    base1.UserName,
    base1.EmailAddress,
    base2.FirstLogin,
    base1.LastLoginTime
from base1 left outer hash
join base2 on base2.Tenant_Id = base1.Tenant_Id and base2.UserId = base1.UserId
