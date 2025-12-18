-- Register Custom data for Attribuutes
select
    rr.RegisterRecord_TenantId Tenant_Id,
    rr.RegisterRecord_Id Id,
    rr.RegisterRecord_Name Name,
    tpc.ThirdPartyControl_Id Custom_Id,
    tpc.ThirdPartyControl_Label Custom_Field,
    tpa.ThirdPartyAttributes_Label Custom_Field_Value

from {{ ref("vwRegisterRecord") }} rr
inner join
    {{ ref("vwIssueCustomAttributeData") }} rrcad
    on rrcad.IssueCustomAttributeData_IssueId = rr.RegisterRecord_Id
inner join
    {{ ref("vwThirdPartyAttributes") }} tpa
    on rrcad.IssueCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
inner join
    {{ ref("vwThirdPartyControl") }} tpc on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
where tpc.ThirdPartyControl_EntityType = 5 and tpa.ThirdPartyAttributes_Label is not null

union all

-- Register Custom data for Free Text
select
    rr.RegisterRecord_TenantId Tenant_Id,
    rr.RegisterRecord_Id Id,
    rr.RegisterRecord_Name Name,
    tpc.ThirdPartyControl_Id Custom_Id,
    tpc.ThirdPartyControl_Label Custom_Field,
    rrtpcft.IssueFreeTextControlData_TextData Custom_Field_Value

from {{ ref("vwRegisterRecord") }} rr
inner join
    {{ ref("vwIssueFreeTextControlData") }} rrtpcft
    on rrtpcft.IssueFreeTextControlData_IssueId = rr.RegisterRecord_Id
join
    {{ ref("vwThirdPartyControl") }} tpc
    on tpc.ThirdPartyControl_Id = rrtpcft.IssueFreeTextControlData_ThirdPartyControlId
where tpc.ThirdPartyControl_EntityType = 5 and rrtpcft.IssueFreeTextControlData_TextData is not null

union all

-- Register Custom data for Date
select
    rr.RegisterRecord_TenantId Tenant_Id,
    rr.RegisterRecord_Id Id,
    rr.RegisterRecord_Name Name,
    tpc.ThirdPartyControl_Id Custom_Id,
    tpc.ThirdPartyControl_Label Custom_Field,
    cast(
        format(rrtpcft.IssueFreeTextControlData_CustomDateValue, 'dd MMM, yyyy') as varchar
    ) Custom_Field_Value

from {{ ref("vwRegisterRecord") }} rr
inner join
    {{ ref("vwIssueFreeTextControlData") }} rrtpcft
    on rrtpcft.IssueFreeTextControlData_IssueId = rr.RegisterRecord_Id
join
    {{ ref("vwThirdPartyControl") }} tpc
    on tpc.ThirdPartyControl_Id = rrtpcft.IssueFreeTextControlData_ThirdPartyControlId
where
    tpc.ThirdPartyControl_EntityType = 5 and rrtpcft.IssueFreeTextControlData_CustomDateValue is not null

union all

-- Register Custom data for Number Value
select
    rr.RegisterRecord_TenantId Tenant_Id,
    rr.RegisterRecord_Id Id,
    rr.RegisterRecord_Name Name,
    tpc.ThirdPartyControl_Id Custom_Id,
    tpc.ThirdPartyControl_Label Custom_Field,
    rrtpcft.IssueFreeTextControlData_NumberValue Custom_Field_Value

from {{ ref("vwRegisterRecord") }} rr
inner join
    {{ ref("vwIssueFreeTextControlData") }} rrtpcft
    on rrtpcft.IssueFreeTextControlData_IssueId = rr.RegisterRecord_Id
join
    {{ ref("vwThirdPartyControl") }} tpc
    on tpc.ThirdPartyControl_Id = rrtpcft.IssueFreeTextControlData_ThirdPartyControlId
where
    tpc.ThirdPartyControl_EntityType = 5 and rrtpcft.IssueFreeTextControlData_NumberValue is not null
    -- and rr.RegisterRecord_Id = 1622
    
