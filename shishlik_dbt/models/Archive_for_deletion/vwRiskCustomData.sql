{# 
DOC START
  - name: vwRiskCustomData
    description: >
      DEPRECATED - previously used in Risks and Risk Assessment Details.  No longer used.
      This view is used to get the custom field and values assigned for a risk and tenant.
    columns:
      - name: ID
DOC END
 #}

{{ config(materialized="view") }}

select *
from {{ ref("vwRisk") }} r
inner join {{ ref("vwRiskCustomAttributeData") }} rcad on rcad.RiskCustomAttributeData_RiskId = r.Risk_Id
inner join
    {{ ref("vwThirdPartyAttributes") }} tpa
    on rcad.RiskCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
inner join
    {{ ref("vwThirdPartyControl") }} tpc on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
where tpc.ThirdPartyControl_EntityType = 2
