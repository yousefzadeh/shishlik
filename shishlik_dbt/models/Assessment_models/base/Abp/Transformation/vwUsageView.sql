select *
from {{ ref("vwUsageLoginMothly") }} ulm

union all

select *
from {{ ref("vwZeroLoginsMonthly") }} zlm
