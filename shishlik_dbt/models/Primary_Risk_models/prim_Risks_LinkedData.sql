select distinct
r.TenantId,
r.Id Risk_Id,
r.TenantEntityUniqueId Risk_IdRef,
r.Name Risk_Name,
a.Title Linked_Assets,
i.Name Linked_Issues,
m.Name Linked_Metrics,
tv.Name Linked_Vendors,
rr.Name Linked_RegisterRecords,
ase.Name Linked_Assessments


from {{ source("risk_models", "Risk") }} r
left join {{ source("risk_models", "RiskAsset") }} ra
on ra.RiskId = r.Id and ra.TenantId = r.TenantId
left join {{ source("assessment_models", "Asset") }} a
on a.Id = ra.AssetId and a.TenantId = ra.TenantId
left join {{ source("issue_models", "IssueRisk") }} ir
on ir.RiskId = r.Id and ir.TenantId = r.TenantId
left join {{ source("issue_models", "Issues") }} i
on i.Id = ir.IssueId and i.TenantId = ir.TenantId
left join {{ source("risk_models", "RiskMetric") }} rm
on rm.RiskId = r.Id
left join {{ source("metric_models", "Metric") }} m
on m.Id = rm.MetricId
left join {{ source("risk_models", "RiskThirdParty") }} rt
on rt.RiskId = r.Id and rt.TenantId = r.TenantId
left join {{ source("tenant_models", "TenantVendor") }} tv
on tv.Id= rt.TenantVendorId
left join {{ source("risk_models", "RiskRegisterRecord") }} rrr
on rrr.RiskId = r.Id and rrr.TenantId = r.TenantId
left join {{ source("register_models", "RegisterRecord") }} rr
on rr.Id = rrr.RegisterRecordId and rr.TenantId = rrr.TenantId
left join {{ source("assessment_models", "AssessmentRisk") }} ar
on ar.RiskId = r.Id and ar.TenantId = r.TenantId
left join {{ source("assessment_models", "Assessment") }} ase
on ase.Id = ar.AssessmentId and ase.TenantId = ar.TenantId

where r.IsDeleted = 0
and r.[Status] != 100