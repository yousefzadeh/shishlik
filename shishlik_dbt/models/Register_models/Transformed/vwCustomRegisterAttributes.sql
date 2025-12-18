-- Note: Showing duplicates for Register Record with no free text and date
-- Register Custom data for Attribuutes
with
    attrib as (
        select
            r.Register_TenantId,
            r.Register_Id,
            r.Register_RegisterName,
            rr.RegisterRecord_Id,
            rr.RegisterRecord_Name,
            rr.RegisterRecord_Description,
            ris.Risk_IdRef + ': ' + ris.Risk_Name LinkedRisk,
            cast(i.Issues_IdRef as varchar(12)) + ': ' + i.Issues_Name LinkedIssues,
            rrol.RegisterRecord_Owner,
            rrol.RegisterRecord_OwnerList,
            t.Tags_Name,
            tpc.ThirdPartyControl_Id Custom_Id,
            tpc.ThirdPartyControl_Label Custom_Field,
            tpa.ThirdPartyAttributes_Label Custom_Field_Value,
            NULL Custom_Field_DateValue,
            tpa.ThirdPartyAttributes_Value Custom_Field_Value_numeric

        from {{ ref("vwRegister") }} r
        left join {{ ref("vwRegisterRecord") }} rr on rr.RegisterRecord_RegisterId = r.Register_Id
        left join
            {{ ref("vwRegisterRecordOwnerList") }} rrol
            on rrol.RegisterRecordOwner_RegisterRecordId = rr.RegisterRecord_Id
        left join
            {{ ref("vwIssueRisk") }} rrr on rrr.IssueRisk_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwRisk") }} ris on ris.Risk_Id = rrr.IssueRisk_RiskId
        left join
            {{ ref("vwIssueRegisterRecord") }} irr on irr.IssueRegisterRecord_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwIssues") }} i on i.Issues_Id = irr.IssueRegisterRecord_LinkedIssueId and i.Issues_Status != 100
        left join {{ ref("vwIssueTag") }} rrt on rrt.IssueTag_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwTags") }} t on t.Tags_ID = rrt.IssueTag_TagId
        left join
            {{ ref("vwIssueCustomAttributeData") }} rrcad
            on rrcad.IssueCustomAttributeData_IssueId = rr.RegisterRecord_Id
        left join
            {{ ref("vwThirdPartyAttributes") }} tpa
            on rrcad.IssueCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
        left join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
        where tpc.ThirdPartyControl_EntityType = 5 and tpa.ThirdPartyAttributes_Label is not null
        
        union all

        select
            r.Register_TenantId,
            r.Register_Id,
            r.Register_RegisterName,
            rr.RegisterRecord_Id,
            rr.RegisterRecord_Name,
            rr.RegisterRecord_Description,
            ris.Risk_IdRef + ': ' + ris.Risk_Name LinkedRisk,
            cast(i.Issues_IdRef as varchar(12)) + ': ' + i.Issues_Name LinkedIssues,
            rrol.RegisterRecord_Owner,
            rrol.RegisterRecord_OwnerList,
            t.Tags_Name,
            tpc.ThirdPartyControl_Id Custom_Id,
            tpc.ThirdPartyControl_Label Custom_Field,
            tpa.ThirdPartyAttributes_Label Custom_Field_Value,
            NULL Custom_Field_DateValue,
            tpa.ThirdPartyAttributes_Value Custom_Field_Value_numeric

        from {{ ref("vwRegister") }} r
        left join {{ ref("vwRegisterRecord") }} rr on rr.RegisterRecord_RegisterId = r.Register_Id
        left join
            {{ ref("vwRegisterRecordOwnerList") }} rrol
            on rrol.RegisterRecordOwner_RegisterRecordId = rr.RegisterRecord_Id
        left join
            {{ ref("vwIssueRisk") }} rrr on rrr.IssueRisk_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwRisk") }} ris on ris.Risk_Id = rrr.IssueRisk_RiskId
        left join
            {{ ref("vwIssueRegisterRecord") }} irr on irr.IssueRegisterRecord_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwIssues") }} i on i.Issues_Id = irr.IssueRegisterRecord_LinkedIssueId and i.Issues_Status != 100
        left join {{ ref("vwIssueTag") }} rrt on rrt.IssueTag_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwTags") }} t on t.Tags_ID = rrt.IssueTag_TagId
        left join
            {{ ref("vwIssueCustomAttributeData") }} rrcad
            on rrcad.IssueCustomAttributeData_IssueId = rr.RegisterRecord_Id
        left join
            {{ ref("vwThirdPartyAttributes") }} tpa
            on rrcad.IssueCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
        left join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
        where tpc.ThirdPartyControl_Id is null
    
    ),
    -- Register Custom data for Free Text
    FreText as (
        select
            r.Register_TenantId,
            r.Register_Id,
            r.Register_RegisterName,
            rr.RegisterRecord_Id,
            rr.RegisterRecord_Name,
            rr.RegisterRecord_Description,
            ris.Risk_IdRef + ': ' + ris.Risk_Name LinkedRisk,
            cast(i.Issues_IdRef as varchar(12)) + ': ' + i.Issues_Name LinkedIssues,
            rrol.RegisterRecord_Owner,
            rrol.RegisterRecord_OwnerList,
            t.Tags_Name,
            tpc.ThirdPartyControl_Id Custom_Id,
            tpc.ThirdPartyControl_Label Custom_Field,
            rrtpcft.IssueFreeTextControlData_TextData Custom_Field_Value,
            NULL Custom_Field_DateValue,
            NULL Custom_Field_Value_numeric

        from {{ ref("vwRegister") }} r
        left join {{ ref("vwRegisterRecord") }} rr on rr.RegisterRecord_RegisterId = r.Register_Id
        left join
            {{ ref("vwRegisterRecordOwnerList") }} rrol
            on rrol.RegisterRecordOwner_RegisterRecordId = rr.RegisterRecord_Id
        left join
            {{ ref("vwIssueRisk") }} rrr on rrr.IssueRisk_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwRisk") }} ris on ris.Risk_Id = rrr.IssueRisk_RiskId
        left join
            {{ ref("vwIssueRegisterRecord") }} irr on irr.IssueRegisterRecord_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwIssues") }} i on i.Issues_Id = irr.IssueRegisterRecord_LinkedIssueId and i.Issues_Status != 100
        left join {{ ref("vwIssueTag") }} rrt on rrt.IssueTag_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwTags") }} t on t.Tags_ID = rrt.IssueTag_TagId
        left join
            {{ ref("vwIssueFreeTextControlData") }} rrtpcft
            on rrtpcft.IssueFreeTextControlData_IssueId = rr.RegisterRecord_Id
        left join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpc.ThirdPartyControl_Id = rrtpcft.IssueFreeTextControlData_ThirdPartyControlId
        where
            tpc.ThirdPartyControl_EntityType = 5
            and rrtpcft.IssueFreeTextControlData_TextData is not null
        
        union all

        select
            r.Register_TenantId,
            r.Register_Id,
            r.Register_RegisterName,
            rr.RegisterRecord_Id,
            rr.RegisterRecord_Name,
            rr.RegisterRecord_Description,
            ris.Risk_IdRef + ': ' + ris.Risk_Name LinkedRisk,
            cast(i.Issues_IdRef as varchar(12)) + ': ' + i.Issues_Name LinkedIssues,
            rrol.RegisterRecord_Owner,
            rrol.RegisterRecord_OwnerList,
            t.Tags_Name,
            tpc.ThirdPartyControl_Id Custom_Id,
            tpc.ThirdPartyControl_Label Custom_Field,
            rrtpcft.IssueFreeTextControlData_TextData Custom_Field_Value,
            NULL Custom_Field_DateValue,
            NULL Custom_Field_Value_numeric

        from {{ ref("vwRegister") }} r
        left join {{ ref("vwRegisterRecord") }} rr on rr.RegisterRecord_RegisterId = r.Register_Id
        left join
            {{ ref("vwRegisterRecordOwnerList") }} rrol
            on rrol.RegisterRecordOwner_RegisterRecordId = rr.RegisterRecord_Id
        left join
            {{ ref("vwIssueRisk") }} rrr on rrr.IssueRisk_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwRisk") }} ris on ris.Risk_Id = rrr.IssueRisk_RiskId
        left join
            {{ ref("vwIssueRegisterRecord") }} irr on irr.IssueRegisterRecord_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwIssues") }} i on i.Issues_Id = irr.IssueRegisterRecord_LinkedIssueId and i.Issues_Status != 100
        left join {{ ref("vwIssueTag") }} rrt on rrt.IssueTag_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwTags") }} t on t.Tags_ID = rrt.IssueTag_TagId
        left join
            {{ ref("vwIssueFreeTextControlData") }} rrtpcft
            on rrtpcft.IssueFreeTextControlData_IssueId = rr.RegisterRecord_Id
        left join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpc.ThirdPartyControl_Id = rrtpcft.IssueFreeTextControlData_ThirdPartyControlId
        where tpc.ThirdPartyControl_Id is null
    
    ),
    -- Register Custom data for Date
    CustomDate as (
        select
            r.Register_TenantId,
            r.Register_Id,
            r.Register_RegisterName,
            rr.RegisterRecord_Id,
            rr.RegisterRecord_Name,
            rr.RegisterRecord_Description,
            ris.Risk_IdRef + ': ' + ris.Risk_Name LinkedRisk,
            cast(i.Issues_IdRef as varchar(12)) + ': ' + i.Issues_Name LinkedIssues,
            rrol.RegisterRecord_Owner,
            rrol.RegisterRecord_OwnerList,
            t.Tags_Name,
            tpc.ThirdPartyControl_Id Custom_Id,
            tpc.ThirdPartyControl_Label Custom_Field,
            NULL Custom_Field_Value,
            rrtpcft.IssueFreeTextControlData_CustomDateValue Custom_Field_DateValue,
            NULL Custom_Field_Value_numeric

        from {{ ref("vwRegister") }} r
        left join {{ ref("vwRegisterRecord") }} rr on rr.RegisterRecord_RegisterId = r.Register_Id
        left join
            {{ ref("vwRegisterRecordOwnerList") }} rrol
            on rrol.RegisterRecordOwner_RegisterRecordId = rr.RegisterRecord_Id
        left join
            {{ ref("vwIssueRisk") }} rrr on rrr.IssueRisk_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwRisk") }} ris on ris.Risk_Id = rrr.IssueRisk_RiskId
        left join
            {{ ref("vwIssueRegisterRecord") }} irr on irr.IssueRegisterRecord_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwIssues") }} i on i.Issues_Id = irr.IssueRegisterRecord_LinkedIssueId and i.Issues_Status != 100
        left join {{ ref("vwIssueTag") }} rrt on rrt.IssueTag_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwTags") }} t on t.Tags_ID = rrt.IssueTag_TagId
        left join
            {{ ref("vwIssueFreeTextControlData") }} rrtpcft
            on rrtpcft.IssueFreeTextControlData_IssueId = rr.RegisterRecord_Id
        left join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpc.ThirdPartyControl_Id = rrtpcft.IssueFreeTextControlData_ThirdPartyControlId
        where
            tpc.ThirdPartyControl_EntityType = 5
            and rrtpcft.IssueFreeTextControlData_CustomDateValue is not null
        
        union all

        select
            r.Register_TenantId,
            r.Register_Id,
            r.Register_RegisterName,
            rr.RegisterRecord_Id,
            rr.RegisterRecord_Name,
            rr.RegisterRecord_Description,
            ris.Risk_IdRef + ': ' + ris.Risk_Name LinkedRisk,
            cast(i.Issues_IdRef as varchar(12)) + ': ' + i.Issues_Name LinkedIssues,
            rrol.RegisterRecord_Owner,
            rrol.RegisterRecord_OwnerList,
            t.Tags_Name,
            tpc.ThirdPartyControl_Id Custom_Id,
            tpc.ThirdPartyControl_Label Custom_Field,
            NULL Custom_Field_Value,
            rrtpcft.IssueFreeTextControlData_CustomDateValue Custom_Field_DateValue,
            NULL Custom_Field_Value_numeric

        from {{ ref("vwRegister") }} r
        left join {{ ref("vwRegisterRecord") }} rr on rr.RegisterRecord_RegisterId = r.Register_Id
        left join
            {{ ref("vwRegisterRecordOwnerList") }} rrol
            on rrol.RegisterRecordOwner_RegisterRecordId = rr.RegisterRecord_Id
        left join
            {{ ref("vwIssueRisk") }} rrr on rrr.IssueRisk_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwRisk") }} ris on ris.Risk_Id = rrr.IssueRisk_RiskId
        left join
            {{ ref("vwIssueRegisterRecord") }} irr on irr.IssueRegisterRecord_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwIssues") }} i on i.Issues_Id = irr.IssueRegisterRecord_LinkedIssueId and i.Issues_Status != 100
        left join {{ ref("vwIssueTag") }} rrt on rrt.IssueTag_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwTags") }} t on t.Tags_ID = rrt.IssueTag_TagId
        left join
            {{ ref("vwIssueFreeTextControlData") }} rrtpcft
            on rrtpcft.IssueFreeTextControlData_IssueId = rr.RegisterRecord_Id
        left join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpc.ThirdPartyControl_Id = rrtpcft.IssueFreeTextControlData_ThirdPartyControlId
        where tpc.ThirdPartyControl_Id is null
    
    ),
    -- Register Custom data for Number Value
    NumValue as (
        select
            r.Register_TenantId,
            r.Register_Id,
            r.Register_RegisterName,
            rr.RegisterRecord_Id,
            rr.RegisterRecord_Name,
            rr.RegisterRecord_Description,
            ris.Risk_IdRef + ': ' + ris.Risk_Name LinkedRisk,
            cast(i.Issues_IdRef as varchar(12)) + ': ' + i.Issues_Name LinkedIssues,
            rrol.RegisterRecord_Owner,
            rrol.RegisterRecord_OwnerList,
            t.Tags_Name,
            tpc.ThirdPartyControl_Id Custom_Id,
            tpc.ThirdPartyControl_Label Custom_Field,
            rrtpcft.IssueFreeTextControlData_NumberValue Custom_Field_Value,
            NULL Custom_Field_DateValue,
            NULL Custom_Field_Value_numeric

        from {{ ref("vwRegister") }} r
        left join {{ ref("vwRegisterRecord") }} rr on rr.RegisterRecord_RegisterId = r.Register_Id
        left join
            {{ ref("vwRegisterRecordOwnerList") }} rrol
            on rrol.RegisterRecordOwner_RegisterRecordId = rr.RegisterRecord_Id
        left join
            {{ ref("vwIssueRisk") }} rrr on rrr.IssueRisk_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwRisk") }} ris on ris.Risk_Id = rrr.IssueRisk_RiskId
        left join
            {{ ref("vwIssueRegisterRecord") }} irr on irr.IssueRegisterRecord_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwIssues") }} i on i.Issues_Id = irr.IssueRegisterRecord_LinkedIssueId and i.Issues_Status != 100
        left join {{ ref("vwIssueTag") }} rrt on rrt.IssueTag_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwTags") }} t on t.Tags_ID = rrt.IssueTag_TagId
        left join
            {{ ref("vwIssueFreeTextControlData") }} rrtpcft
            on rrtpcft.IssueFreeTextControlData_IssueId = rr.RegisterRecord_Id
        left join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpc.ThirdPartyControl_Id = rrtpcft.IssueFreeTextControlData_ThirdPartyControlId
        where
            tpc.ThirdPartyControl_EntityType = 5
            and rrtpcft.IssueFreeTextControlData_NumberValue is not null
        
        union all

        select
            r.Register_TenantId,
            r.Register_Id,
            r.Register_RegisterName,
            rr.RegisterRecord_Id,
            rr.RegisterRecord_Name,
            rr.RegisterRecord_Description,
            ris.Risk_IdRef + ': ' + ris.Risk_Name LinkedRisk,
            cast(i.Issues_IdRef as varchar(12)) + ': ' + i.Issues_Name LinkedIssues,
            rrol.RegisterRecord_Owner,
            rrol.RegisterRecord_OwnerList,
            t.Tags_Name,
            tpc.ThirdPartyControl_Id Custom_Id,
            tpc.ThirdPartyControl_Label Custom_Field,
            rrtpcft.IssueFreeTextControlData_NumberValue Custom_Field_Value,
            NULL Custom_Field_DateValue,
            NULL Custom_Field_Value_numeric

        from {{ ref("vwRegister") }} r
        left join {{ ref("vwRegisterRecord") }} rr on rr.RegisterRecord_RegisterId = r.Register_Id
        left join
            {{ ref("vwRegisterRecordOwnerList") }} rrol
            on rrol.RegisterRecordOwner_RegisterRecordId = rr.RegisterRecord_Id
        left join
            {{ ref("vwIssueRisk") }} rrr on rrr.IssueRisk_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwRisk") }} ris on ris.Risk_Id = rrr.IssueRisk_RiskId
        left join
            {{ ref("vwIssueRegisterRecord") }} irr on irr.IssueRegisterRecord_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwIssues") }} i on i.Issues_Id = irr.IssueRegisterRecord_LinkedIssueId and i.Issues_Status != 100
        left join {{ ref("vwIssueTag") }} rrt on rrt.IssueTag_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwTags") }} t on t.Tags_ID = rrt.IssueTag_TagId
        left join
            {{ ref("vwIssueFreeTextControlData") }} rrtpcft
            on rrtpcft.IssueFreeTextControlData_IssueId = rr.RegisterRecord_Id
        left join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpc.ThirdPartyControl_Id = rrtpcft.IssueFreeTextControlData_ThirdPartyControlId
        where tpc.ThirdPartyControl_Id is null
    
    ),
    uni as (
        select *
        from attrib

        union all

        select *
        from FreText

        union all

        select *
        from CustomDate

        union all

        select *
        from NumValue
    )

select distinct uni.*
from uni