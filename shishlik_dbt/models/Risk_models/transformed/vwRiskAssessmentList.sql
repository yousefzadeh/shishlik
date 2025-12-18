-- for Risk Details Self-Service View
with
    base as (
        select vrlawd.Risk_TenantId, vrlawd.Risk_Id, ass.Assessment_Name
        from {{ ref("vwRiskLinkedAssessmentWorkflowDetail") }} vrlawd
        join {{ ref("vwAssessment") }} ass on vrlawd.Assessment_ID = ass.Assessment_ID
        group by vrlawd.Risk_TenantId, vrlawd.Risk_Id, ass.Assessment_Name
    )
select
    q.Risk_TenantId,
    q.Risk_Id,
    string_agg(CAST(q.Assessment_Name as nvarchar(MAX)), ', ') AssessmentList
from base as q
group by q.Risk_TenantId, q.Risk_Id
