-- Register Custom data for Attribuutes
select
    rr.RegisterRecord_TenantId Tenant_Id,
    rr.RegisterRecord_Id Id,
    rr.RegisterRecord_Name Name,
    tpc.ThirdPartyControl_Id Custom_Id,
    tpc.ThirdPartyControl_Label Custom_Field,
    tpa.ThirdPartyAttributes_Label Custom_Field_Value

from {{ ref("vwRegister") }} r
join {{ ref("vwRegisterRecord") }} rr
on rr.RegisterRecord_RegisterId = r.Register_Id
inner join
    {{ ref("vwIssueCustomAttributeData") }} rrcad
    on rrcad.IssueCustomAttributeData_IssueId = rr.RegisterRecord_Id
inner join
    {{ ref("vwThirdPartyAttributes") }} tpa
    on rrcad.IssueCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
inner join
    {{ ref("vwThirdPartyControl") }} tpc on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
where
    tpc.ThirdPartyControl_EntityType = 5 and tpa.ThirdPartyAttributes_Label is not null
    -- and rr.RegisterRecord_Id = 1622
    
