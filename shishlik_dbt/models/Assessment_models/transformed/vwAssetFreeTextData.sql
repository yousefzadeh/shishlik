-- Asset Custom data for Attributes
select
    a.Asset_TenantId Tenant_Id,
    a.Asset_Id Id,
    a.Asset_Title Name,
    tpc.ThirdPartyControl_Id Custom_Id,
    tpc.ThirdPartyControl_Label Custom_Field,
    tpa.ThirdPartyAttributes_Label Custom_Field_Value
from {{ ref("vwAsset") }} a
inner join {{ ref("vwIssueCustomAttributeData") }} acad on acad.IssueCustomAttributeData_IssueId = a.Asset_Id
inner join
    {{ ref("vwThirdPartyAttributes") }} tpa
    on acad.IssueCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
inner join
    {{ ref("vwThirdPartyControl") }} tpc
    on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
    and tpc.ThirdPartyControl_Enabled = 1
where tpc.ThirdPartyControl_EntityType = 1 and tpa.ThirdPartyAttributes_Label is not null

union all

-- Asset Custom data for Free Text
select
    a.Asset_TenantId Tenant_Id,
    a.Asset_Id Id,
    a.Asset_Title Name,
    tpc.ThirdPartyControl_Id Custom_Id,
    tpc.ThirdPartyControl_Label Custom_Field,
    atpcft.IssueFreeTextControlData_TextData Custom_Field_Value
from {{ ref("vwAsset") }} a
join {{ ref("vwIssueFreeTextControlData") }} atpcft on atpcft.IssueFreeTextControlData_IssueId = a.Asset_Id
inner join
    {{ ref("vwThirdPartyControl") }} tpc
    on tpc.ThirdPartyControl_Id = atpcft.IssueFreeTextControlData_ThirdPartyControlId
    and tpc.ThirdPartyControl_Enabled = 1
where tpc.ThirdPartyControl_EntityType = 1 and atpcft.IssueFreeTextControlData_TextData is not null

union all

-- Asset Custom data for Date
select
    a.Asset_TenantId Tenant_Id,
    a.Asset_Id Id,
    a.Asset_Title Name,
    tpc.ThirdPartyControl_Id Custom_Id,
    tpc.ThirdPartyControl_Label Custom_Field,
    cast(format(atpcft.IssueFreeTextControlData_CustomDateValue, 'dd MMM, yyyy') as varchar) Custom_Field_Value
from {{ ref("vwAsset") }} a
join {{ ref("vwIssueFreeTextControlData") }} atpcft on atpcft.IssueFreeTextControlData_IssueId = a.Asset_Id
inner join
    {{ ref("vwThirdPartyControl") }} tpc
    on tpc.ThirdPartyControl_Id = atpcft.IssueFreeTextControlData_ThirdPartyControlId
    and tpc.ThirdPartyControl_Enabled = 1
where tpc.ThirdPartyControl_EntityType = 1 and atpcft.IssueFreeTextControlData_CustomDateValue is not null
