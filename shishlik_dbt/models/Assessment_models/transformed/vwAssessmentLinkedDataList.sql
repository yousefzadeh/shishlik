with assess as (
    select
        a.Assessment_ID,
        a.Assessment_Name,
        a.Assessment_TenantId,
        a.Assessment_AuthorityId,
        a.Assessment_PolicyId,
        a.Assessment_UpdateTime 
    from {{ ref("vwAssessment") }} a
)
---
, ris as (
    select 
        ar.AssessmentScopeRisk_TenantId,
        ar.AssessmentScopeRisk_AssessmentId,
        STRING_AGG(r.Risk_Name, ', ') Assessment_LinkedRiskList,
        max(greatest(AssessmentScopeRisk_UpdateTime, Risk_UpdateTime)) Max_UpdateTime

    from {{ ref("vwAssessmentScopeRisk") }} ar
    join {{ ref("vwRisk") }} r
    on r.Risk_Id = ar.AssessmentScopeRisk_RiskId
    group by
        ar.AssessmentScopeRisk_TenantId,
        ar.AssessmentScopeRisk_AssessmentId
)
---
, iss as (
    select 
        ar.AssessmentScopeRegisterItem_TenantId,
        ar.AssessmentScopeRegisterItem_AssessmentId,
        STRING_AGG(i.Issues_Name, ', ') Assessment_LinkedIssueList,
        max(greatest(AssessmentScopeRegisterItem_UpdateTime, Issues_UpdatedTime)) Max_UpdateTime

    from {{ ref("vwAssessmentScopeRegisterItem") }} ar
    join {{ ref("vwIssues") }} i
    on i.Issues_Id = ar.AssessmentScopeRegisterItem_RegisterItemId
    group by
        ar.AssessmentScopeRegisterItem_TenantId,
        ar.AssessmentScopeRegisterItem_AssessmentId
)
---
, ast as (
    select 
        ar.AssessmentScopeRegisterItem_TenantId,
        ar.AssessmentScopeRegisterItem_AssessmentId,
        STRING_AGG(a.Asset_Title, ', ') Assessment_LinkedAssetList,
        max(greatest(AssessmentScopeRegisterItem_UpdateTime, Asset_UpdatedTime)) Max_UpdateTime

    from {{ ref("vwAssessmentScopeRegisterItem") }} ar
    join {{ ref("vwAsset") }} a
    on a.Asset_Id = ar.AssessmentScopeRegisterItem_RegisterItemId
    group by
        ar.AssessmentScopeRegisterItem_TenantId,
        ar.AssessmentScopeRegisterItem_AssessmentId
)
---
, reg_rcd as (
    select 
        ar.AssessmentScopeRegisterItem_TenantId,
        ar.AssessmentScopeRegisterItem_AssessmentId,
        STRING_AGG(rr.RegisterRecord_Name, ', ') Assessment_LinkedRegisterRecordList,
        max(greatest(AssessmentScopeRegisterItem_UpdateTime, RegisterRecord_UpdatedTime)) Max_UpdateTime

    from {{ ref("vwAssessmentScopeRegisterItem") }} ar
    join {{ ref("vwRegisterRecord") }} rr
    on rr.RegisterRecord_Id = ar.AssessmentScopeRegisterItem_RegisterItemId
    group by
        ar.AssessmentScopeRegisterItem_TenantId,
        ar.AssessmentScopeRegisterItem_AssessmentId
)
---
, reg as (
    select 
        ar.AssessmentScopeRegisterItem_TenantId,
        ar.AssessmentScopeRegisterItem_AssessmentId,
        rr.RegisterRecord_RegisterId,
        max(greatest(AssessmentScopeRegisterItem_UpdateTime, RegisterRecord_UpdatedTime)) Max_UpdateTime

    from {{ ref("vwAssessmentScopeRegisterItem") }} ar
    join {{ ref("vwRegisterRecord") }} rr
    on rr.RegisterRecord_Id = ar.AssessmentScopeRegisterItem_RegisterItemId
    group by
        ar.AssessmentScopeRegisterItem_TenantId,
        ar.AssessmentScopeRegisterItem_AssessmentId,
        rr.RegisterRecord_RegisterId
)
---
, reg2 as (
    select 
        rr.AssessmentScopeRegisterItem_TenantId,
        rr.AssessmentScopeRegisterItem_AssessmentId,
        STRING_AGG(r.Register_RegisterName, ', ') Assessment_LinkedRegisterList,
        max(greatest(Max_UpdateTime, Register_UpdateTime)) Max_UpdateTime

    from reg rr
    join {{ ref("vwRegister") }} r
    on r.Register_Id = rr.RegisterRecord_RegisterId
    group by
        rr.AssessmentScopeRegisterItem_TenantId,
        rr.AssessmentScopeRegisterItem_AssessmentId
)

select
    a.Assessment_ID,
    a.Assessment_Name,
    a.Assessment_TenantId,
    a.Assessment_AuthorityId,
    a.Assessment_PolicyId,
    r.Assessment_LinkedRiskList,
    i.Assessment_LinkedIssueList,
    ast.Assessment_LinkedAssetList,
    rg.Assessment_LinkedRegisterList,
    rr.Assessment_LinkedRegisterRecordList,
    greatest(a.Assessment_UpdateTime,r.Max_UpdateTime,i.Max_UpdateTime,ast.Max_UpdateTime,rr.Max_UpdateTime,rg.Max_UpdateTime) Assessment_UpdateTime 

from assess a
left join ris r on r.AssessmentScopeRisk_AssessmentId = a.Assessment_ID
left join iss i on i.AssessmentScopeRegisterItem_AssessmentId = a.Assessment_ID
left join ast on ast.AssessmentScopeRegisterItem_AssessmentId = a.Assessment_ID
left join reg_rcd rr on rr.AssessmentScopeRegisterItem_AssessmentId = a.Assessment_ID
left join reg2 rg on rg.AssessmentScopeRegisterItem_AssessmentId = a.Assessment_ID