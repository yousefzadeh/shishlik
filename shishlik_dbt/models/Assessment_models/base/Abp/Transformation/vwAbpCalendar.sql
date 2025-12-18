--WITH seq AS 
--(
--  SELECT 0 n UNION ALL SELECT n + 1 FROM seq
--  WHERE n < DATEDIFF(DAY, '20100101', DATEADD(DAY, -1, DATEADD(YEAR, 30, '20190101')))
--),
--d AS 
--(
--  SELECT DATEADD(DAY, n, '20100101') d FROM seq
--),
with base2 as (
        select distinct Id Tenant_Id, DATEADD(month, nbr - 1, '20190901') d
        -- cast(Format(DATEADD(Month, nbr - 1, '20000101'), 'MMM, yyyy') as varchar) LoginMonth
        from
            (
                select c.Id, ROW_NUMBER() over (order by c.Id) as nbr
                from dbo.AbpTenants c
            ) nbrs
    )
,src AS
(
  SELECT
    TheDate         = CONVERT(datetime, d)
    {# TheDay          = DATEPART(DAY,       d),
    TheDayName      = DATENAME(WEEKDAY,   d),
    TheWeek         = DATEPART(WEEK,      d),
    TheISOWeek      = DATEPART(ISO_WEEK,  d),
    TheDayOfWeek    = DATEPART(WEEKDAY,   d),
    TheMonth        = DATEPART(MONTH,     d),
    TheMonthName    = DATENAME(MONTH,     d),
    TheQuarter      = DATEPART(Quarter,   d),
    TheYear         = DATEPART(YEAR,      d),
    TheFirstOfMonth = DATEFROMPARTS(YEAR(d), MONTH(d), 1),
    TheLastOfYear   = DATEFROMPARTS(YEAR(d), 12, 31),
    TheDayOfYear    = DATEPART(DAYOFYEAR, d) #}
  FROM base2
)

SELECT at2.AbpTenants_Id,-- at2.AbpTenants_CreationTime, 
src.* FROM src
cross apply {{ ref("vwAbpTenants") }} at2
where src.TheDate >= at2.AbpTenants_CreationTime and src.TheDate <= coalesce(AbpTenants_DeletionTime, getdate())
and at2.AbpTenants_ServiceProviderId is NULL