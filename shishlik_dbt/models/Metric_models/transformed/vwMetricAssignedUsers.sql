select
m.Metric_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(mo.MetricOwner_LastModificationTime, mo.MetricOwner_CreationTime) as Record_LastModificationTime,
'Owner' Module,
'Metrics' AssignedItemType,
m.Metric_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from {{ ref("vwMetric") }} m
join {{ ref("vwMetricOwner") }} mo
on mo.MetricOwner_MetricId = m.Metric_Id
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = mo.MetricOwner_UserId