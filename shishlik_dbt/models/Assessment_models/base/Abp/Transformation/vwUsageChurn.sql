--Updated Retention sql;

with by_month as (
    SELECT DISTINCT
      --  VWABPUSERLOGINATTEMPTS.AbpUserLoginAttempts_CreationTimeMonth CreationTimeMonth,
      VWABPCALENDAR.TheDate CreationMonthDate,
       VWABPTENANTS.AbpTenants_Name,
       VWABPTENANTS.AbpTenants_Id,
       ae.AbpEditions_DisplayName
    FROM {{ ref("vwAbpTenants") }} AS VWABPTENANTS
    LEFT OUTER JOIN {{ ref("vwAbpCalendar") }} AS VWABPCALENDAR
   ON (
   VWABPTENANTS.AbpTenants_Id = VWABPCALENDAR.AbpTenants_Id
   )
    left join {{ ref("vwAbpEditions") }} ae on ae.AbpEditions_Id = VWABPTENANTS.AbpTenants_EditionId
    WHERE (
       VWABPTENANTS.AbpTenants_ServiceProviderId IS NULL
    )
    and VWABPTENANTS.AbpTenants_IsInternal = 0
   --  AND DATEADD(MONTH, DATEDIFF(MONTH, 0, vwAbpUserLoginAttempts.AbpUserLoginAttempts_CreationTime), 0) BETWEEN CAST('20230101 00:00:00.000' as DATETIME) AND CAST('20230731 23:59:59.997' as DATETIME)
      --  AND VWABPUSERLOGINATTEMPTS.AbpUserLoginAttempts_CreationTimeMonth IS NOT NULL 
), 
with_lag as (
    SELECT 
       AbpTenants_Name,
       AbpTenants_Id,
       AbpEditions_DisplayName,
       CreationMonthDate, 
       lag(CreationMonthDate) over (
       partition by 
           AbpTenants_Name
       order by 
           AbpTenants_Name,
           CreationMonthDate
    --   rows between 1 preceding and current row
       ) prev_login_monthdate,
       lead(CreationMonthDate) over (
       partition by 
           AbpTenants_Name
       order by 
           AbpTenants_Name,
           CreationMonthDate
    --   rows between 1 preceding and current row
       ) next_login_monthdate
    FROM by_month
),
next_month_is_null as (
   select 
   AbpTenants_Name,
   AbpTenants_Id,
   AbpEditions_DisplayName,
   CreationMonthDate prev_login_monthdate,
   dateadd(month,1,CreationMonthDate) CreationMonthDate,
   NULL next_login_monthdate
   from with_lag
   where next_login_monthdate is NULL OR datediff(month, CreationMonthDate, next_login_monthdate) != 1
),   
all_rows as (
    select 
    AbpTenants_Name,
    AbpTenants_Id,
    AbpEditions_DisplayName,
    prev_login_monthdate,
    CreationMonthDate,
    next_login_monthdate,
    1 n
    from with_lag
    union all
    select 
    AbpTenants_Name,
    AbpTenants_Id,
    AbpEditions_DisplayName,
    prev_login_monthdate,
    CreationMonthDate,
    next_login_monthdate,
    0 n
    from next_month_is_null
),
-- calculate flags using all_rows
with_diff as (
    select 
       AbpTenants_Name,
       AbpTenants_Id,
       AbpEditions_DisplayName,
       prev_login_monthdate,
       datediff(month, prev_login_monthdate, CreationMonthDate) prev_month_diff, -- date diff of previous transaction
       CreationMonthDate,
       next_login_monthdate,
       datediff(month, CreationMonthDate, next_login_monthdate) next_month_diff, -- date diff of next transaction
       n
    from all_rows
)
, with_flag as(
    select 
       AbpTenants_Name,
       AbpTenants_Id,
       AbpEditions_DisplayName,
       CreationMonthDate,
       coalesce(lag(n,1) over (partition by AbpTenants_Id order by CreationMonthDate),0) prev_n,
       n       
    from with_diff
)
select 
   AbpTenants_Name,
   AbpTenants_Id,
   AbpEditions_DisplayName,
   cast(Format(CreationMonthDate, 'MMM, yyyy') as varchar) CreationTimeMonth,
   CreationMonthDate,
   case when prev_n + n > 0 then 1 else 0 end is_consecutive_2month_login
from with_flag