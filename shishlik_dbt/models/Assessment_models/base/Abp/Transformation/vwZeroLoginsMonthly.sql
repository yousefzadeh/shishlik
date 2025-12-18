with
    base1 as (
        select distinct
            at2.Id Tenant_Id,
            at2.Name TenantName,
            ae.DisplayName Edition,
            at2.CreationTime,
            au2.Id UserId,
            au2.Name + ' ' + au2.Surname UserName,
            au2.CreationTime UserCreationDate,
            au2.EmailAddress,
            au2.LastLoginTime

        from {{ source("assessment_models", "AbpTenants") }} at2
        left join {{ source("assessment_models", "AbpUsers") }} au2 on au2.TenantId = at2.Id
        left join {{ source("assessment_models", "AbpEditions") }} ae on ae.Id = at2.EditionId
        where
            at2.Id not in (
                select distinct at2.Id

                from {{ source("assessment_models", "AbpUserLoginAttempts") }} aula
                left join {{ source("assessment_models", "AbpTenants") }} at2 on at2.Id = aula.TenantId
                left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = aula.UserId
                left join {{ source("assessment_models", "AbpEditions") }} ae on ae.Id = at2.EditionId

                where aula. [Result] = 1 and at2.Id is not null
            )
            and at2.IsInternal = 0
    ),
    base2 as (
        select distinct Id Tenant_Id, DATEADD(Month, nbr - 1, '20000101') Date_Time
        -- cast(Format(DATEADD(Month, nbr - 1, '20000101'), 'MMM, yyyy') as varchar) LoginMonth
        from
            (
                select c.Id, ROW_NUMBER() over (order by c.Id) as nbr
                from {{ source("assessment_models", "AbpTenants") }} c
            ) nbrs
    ),
    base3 as (
        select distinct at2.Id Tenant_Id, base2.Date_Time

        from {{ source("assessment_models", "AbpTenants") }} at2 cross apply base2

        where base2.Date_Time between at2.CreationTime and getdate()
    )

select distinct
    'ZeroLogged' Logintype,
    base1.Tenant_Id,
    DATEADD(MONTH, DATEDIFF(MONTH, 0, base3.Date_Time), 0) GenDate,
    base3.Date_Time,
    cast(Format(base3.Date_Time, 'MMM, yyyy') as varchar) LoginMonth,
    year(base3.Date_Time) Year,
    month(base3.Date_Time) Month,
    base1.TenantName,
    base1.Edition,
    base1.UserId,
    base1.UserName,
    base1.EmailAddress,
    case when base1.LastLoginTime is null then null else base1.UserCreationDate end FirstLogin,
    base1.LastLoginTime

from base1 left outer hash
join
    base3 on base3.Tenant_Id = base1.Tenant_Id

    -- where base1.Tenant_Id = 1838
    -- and base1.Tenant_Id in (1384, 1838)
    
