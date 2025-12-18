{{ config(materialized="view") }}

select *
from {{ ref("vwIssues") }} i
inner join {{ ref("vwIssueCustomAttributeData") }} icad on i.Issues_Id = icad.IssueCustomAttributeData_IssueId
inner join
    {{ ref("vwThirdPartyAttributes") }} tpa
    on icad.IssueCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
inner join
    {{ ref("vwThirdPartyControl") }} tpc on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
where tpc.ThirdPartyControl_EntityType = 6 and tpc.ThirdPartyControl_Label in ('Type')
