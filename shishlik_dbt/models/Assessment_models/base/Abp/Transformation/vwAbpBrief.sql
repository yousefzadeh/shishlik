with
    base1 as (
        select distinct
            at2.id tenant_id,
            at2.name tenantname,
            ae.displayname edition,
            at2.creationtime,
            au2.id,
            au2.name + ' ' + au2.surname username,
            au2.creationtime usercreationdate,
            au2.emailaddress,
            au2.lastlogintime

        from {{ source("assessment_models", "AbpTenants") }} at2
        left join
            {{ source("assessment_models", "AbpUsers") }} au2 on au2.tenantid = at2.id
        left join
            {{ source("assessment_models", "AbpEditions") }} ae on ae.id = at2.editionid

    ),
    base2 as (
        select distinct id tenant_id, dateadd(month, nbr - 1, '20000101') date_time
        -- cast(Format(DATEADD(Month, nbr - 1, '20000101'), 'MMM, yyyy') as varchar)
        -- LoginMonth
        from
            (
                select c.id, row_number() over (order by c.id) as nbr
                from {{ source("assessment_models", "AbpTenants") }} c
            ) nbrs
    ),
    base3 as (
        select distinct at2.id tenant_id, base2.date_time

        from {{ source("assessment_models", "AbpTenants") }} at2 cross apply base2

        where base2.date_time between at2.creationtime and getdate()
    )

select distinct
    'Logged' logintype,
    base1.tenant_id,
    dateadd(month, datediff(month, 0, base3.date_time), 0) gendate,
    base3.date_time,
    cast(format(base3.date_time, 'MMM, yyyy') as varchar) loginmonth,
    year(base3.date_time) year,
    month(base3.date_time) month,
    base1.tenantname,
    base1.edition,
    base1.username,
    base1.emailaddress,
    case
        when base1.lastlogintime is null then null else base1.usercreationdate
    end firstlogin,
    base1.lastlogintime

from base1 left outer hash
join base3 on base3.tenant_id = base1.tenant_id
