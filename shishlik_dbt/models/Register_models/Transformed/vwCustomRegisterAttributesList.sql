with
    main as (
        select *
        from {{ ref("vwRegister") }} r
        left join {{ ref("vwRegisterRecord") }} rr on rr.RegisterRecord_RegisterId = r.Register_Id
    -- where cra.Register_TenantId = 1384
    -- and RegisterRecord_Id = 1791
    ),
    TagList as (
        select distinct
            rr.RegisterRecord_Id,
            left(STRING_AGG(cast(t.Tags_Name as nvarchar(max)), ', '), 4000) RegisterRecord_TagList
        from {{ ref("vwRegister") }} r
        left join {{ ref("vwRegisterRecord") }} rr on rr.RegisterRecord_RegisterId = r.Register_Id
        left join {{ ref("vwIssueTag") }} it on it.IssueTag_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwTags") }} t on t.Tags_ID = it.IssueTag_TagId
        -- where r.Register_TenantId = 1384
        -- and rr.RegisterRecord_Id = 1791
        group by rr.RegisterRecord_Id
    ),
    LinkedRisksList as (
        select distinct
            rr.RegisterRecord_Id,
            left(
                STRING_AGG(cast(ris.Risk_IdRef + ': ' + ris.Risk_Name as nvarchar(max)), ', '), 4000
            ) RegisterRecord_RisksList
        from {{ ref("vwRegister") }} r
        left join {{ ref("vwRegisterRecord") }} rr on rr.RegisterRecord_RegisterId = r.Register_Id
        left join
            {{ ref("vwIssueRisk") }} ir on ir.IssueRisk_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwRisk") }} ris on ris.Risk_Id = ir.IssueRisk_RiskId
        -- where r.Register_TenantId = 1384
        -- and rr.RegisterRecord_Id = 1791
        group by rr.RegisterRecord_Id
    ),
    LinkedIssuessList as (
        select distinct
            rr.RegisterRecord_Id,
            left(
                STRING_AGG(cast(i.Issues_IdRef as varchar(12)) + ': ' + i.Issues_Name, ', '), 4000
            ) RegisterRecord_IssuesList
        from {{ ref("vwRegister") }} r
        left join {{ ref("vwRegisterRecord") }} rr on rr.RegisterRecord_RegisterId = r.Register_Id
        left join
            {{ ref("vwIssueRegisterRecord") }} irr on irr.IssueRegisterRecord_IssueId = rr.RegisterRecord_Id
        left join {{ ref("vwIssues") }} i on i.Issues_Id = irr.IssueRegisterRecord_LinkedIssueId
        group by rr.RegisterRecord_Id
    ),
    CustomFieldValuesList as (
        select distinct
            cra.Id, cra.Custom_Field + ' = "' + string_agg(cra.Custom_Field_Value, '","') + '"' CustomFieldList
        from {{ ref("vwRegisterRecordFreeTextData") }} cra
        group by cra.Id, cra.Custom_Field
    ),
    field_value_list as (
        select distinct Id, '[ ' + string_agg(CustomFieldList, '] [') + ' ]' RegisterRecord_CustomValueList
        from CustomFieldValuesList
        group by Id
    )

select distinct
    m.Register_TenantId,
    m.Register_Id,
    m.Register_RegisterName,
    m.RegisterRecord_Id,
    m.RegisterRecord_Name,
    tl.RegisterRecord_TagList,
    rl.RegisterRecord_RisksList,
    il.RegisterRecord_IssuesList,
    cvl.RegisterRecord_CustomValueList

from main m
left join TagList tl on tl.RegisterRecord_Id = m.RegisterRecord_Id
left join LinkedRisksList rl on rl.RegisterRecord_Id = m.RegisterRecord_Id
left join LinkedIssuessList il on il.RegisterRecord_Id = m.RegisterRecord_Id
left join
    field_value_list cvl on cvl.Id = m.RegisterRecord_Id
    -- where m.Register_TenantId = 1384
    -- and m.RegisterRecord_Id = 1791
    
