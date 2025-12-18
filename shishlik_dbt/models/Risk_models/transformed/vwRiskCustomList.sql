with
    risk_domain as (
        select r.Risk_TenantId, r.Risk_Id, tpa.ThirdPartyAttributes_Label domain
        from {{ ref("vwRisk") }} r
        join
            {{ ref("vwRiskCustomAttributeData") }} rcad
            on rcad.RiskCustomAttributeData_RiskId = r.Risk_Id
        join
            {{ ref("vwThirdPartyAttributes") }} tpa
            on tpa.ThirdPartyAttributes_Id
            = rcad.RiskCustomAttributeData_ThirdPartyAttributesId
        join
            {{ source("issue_models", "ThirdPartyControl") }} tpc
            on tpc.Id = tpa.ThirdPartyAttributes_ThirdPartyControlId
            and tpc.Name = 'RiskDomain'
        where
            -- r.Risk_IsCurrent = 1
            -- and 
            tpc.Enabled = 1
            and tpc.EntityType = 2
            and tpc.IsDeleted = 0
        group by r.Risk_TenantId, r.Risk_Id, tpa.ThirdPartyAttributes_Label
    ),
    child_domain as (
        select r.Risk_TenantId, r.Risk_Id, tpa.ThirdPartyAttributes_Label domain
        from {{ ref("vwRisk") }} r
        join
            {{ ref("vwRiskCustomAttributeData") }} rcad
            on rcad.RiskCustomAttributeData_RiskId = r.Risk_Id
        join
            {{ ref("vwThirdPartyAttributes") }} tpa
            on tpa.ThirdPartyAttributes_Id
            = rcad.RiskCustomAttributeData_ThirdPartyAttributesId
        join
            {{ source("issue_models", "ThirdPartyControl") }} tpc
            on tpc.Id = tpa.ThirdPartyAttributes_ThirdPartyControlId
            and tpc.Name is null
        join
            {{ source("issue_models", "ThirdPartyControl") }} tpc2
            on tpc2.Id = tpc.ParentThirdPartyControlId
        where
            -- r.Risk_IsCurrent = 1
            -- and 
            tpc.Enabled = 1
            and tpc.EntityType = 2
            and tpc.IsDeleted = 0
            and tpc2.ParentThirdPartyControlId is null
        group by r.Risk_TenantId, r.Risk_Id, tpa.ThirdPartyAttributes_Label
    ),
    grandchild_domain as (
        select r.Risk_TenantId, r.Risk_Id, tpa.ThirdPartyAttributes_Label domain
        from {{ ref("vwRisk") }} r
        join
            {{ ref("vwRiskCustomAttributeData") }} rcad
            on rcad.RiskCustomAttributeData_RiskId = r.Risk_Id
        join
            {{ ref("vwThirdPartyAttributes") }} tpa
            on tpa.ThirdPartyAttributes_Id
            = rcad.RiskCustomAttributeData_ThirdPartyAttributesId
        join
            {{ source("issue_models", "ThirdPartyControl") }} tpc
            on tpc.Id = tpa.ThirdPartyAttributes_ThirdPartyControlId
        join
            {{ source("issue_models", "ThirdPartyControl") }} tpc2
            on tpc.ParentThirdPartyControlId = tpc2.Id
            and tpc2.Name is null
        where
            -- r.Risk_IsCurrent = 1
            -- and 
            tpc.Enabled = 1
            and tpc.EntityType = 2
            and tpc2.IsDeleted = 0
        group by r.Risk_TenantId, r.Risk_Id, tpa.ThirdPartyAttributes_Label
    ),
    all_domains as (
        select *
        from risk_domain
        union all
        select *
        from child_domain
        union all
        select *
        from grandchild_domain
    ),
    custom_field as (
        select distinct rcd.Risk_TenantId, rcd.Risk_Id, rcd.CustomLabel
        from {{ ref("vwRisksCustomData") }} rcd
    ),
    custom_Value as (
        select distinct rcd.Risk_TenantId, rcd.Risk_Id, rcd.CustomLabel, rcd.Value
        from {{ ref("vwRisksCustomData") }} rcd
        where
            rcd.Value not in (
                select domain
                from all_domains
                where all_domains.Risk_TenantId = rcd.Risk_TenantId
            )
    ),
    value_list as (
        select
            Risk_TenantId,
            Risk_Id,
            CustomLabel + ' = "' + string_agg([Value], '","') + '"' field_list
        from custom_value
        group by Risk_TenantId, Risk_Id, CustomLabel
    ),
    field_value_list as (
        select
            Risk_TenantId,
            Risk_Id,
            '[ ' + string_agg(field_list, '] [') + ' ]' field_value_list
        from value_list
        group by Risk_TenantId, Risk_Id
    )
select *
from field_value_list
