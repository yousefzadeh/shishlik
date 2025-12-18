{# 
DOC START
  - name: Filter_TemplateRespondingTeam
    description: |
        This view is used in a custom SQL filter for the following View:
        - Question Based Assessment Answer Details
               
        It lists One row per 
        - Template (QBA only)
        - Responding Team for all Completed Assessments created from this Template

    columns:
      - name: Tenant_Id
        description: ID of Login Tenant for Access Filtering
      - name: Template_Name
        description: Name of the Assessment Template 
      - name: RespondingTeam
        description: Name of the Responding Team for all assessments created from this template
      - name: QuestionType
        description: |
          QuestionType specifies if the Assessment is Risk or Weighted Score
          1 for Risk Assessment
          2 for Weighted Score Assessment

DOC END    
#}
with final as (
    SELECT DISTINCT
        asst.Assessment_TenantId Tenant_Id,
        asst.Assessment_Name Template_Name,
        tv.TenantVendor_Name RespondingTeam,
        ass.Assessment_QuestionType QuestionType,
        MAX(GREATEST(cast(asst.Assessment_UpdateTime as datetime2), cast(ass.Assessment_UpdateTime as datetime2),cast( tv.TenantVendor_UpdateTime as datetime2))) OVER (PARTITION BY asst.Assessment_TenantId,asst.Assessment_Name,tv.TenantVendor_Name,ass.Assessment_QuestionType) AS Filter_UpdateTime
    FROM
        {{ ref("vwAssessment") }} asst
    INNER JOIN
        {{ ref("vwAssessment") }} ass
        ON
            ass.Assessment_TenantId = asst.Assessment_TenantId
            AND	ass.Assessment_CreatedFromTemplateId = asst.Assessment_ID
            AND	ass.Assessment_IsTemplate = 0
            AND	ass.Assessment_WorkFlowId = 0
    INNER JOIN
        {{ ref("vwTenantVendor") }} tv
        ON
            tv.TenantVendor_TenantId = ass.Assessment_TenantId
            AND	tv.TenantVendor_Id = ass.Assessment_TenantVendorId
    WHERE
    asst.Assessment_IsTemplate = 1
    and ass.Assessment_IsArchived =0
    AND ass.Assessment_Status in (4, 6)
)
SELECT DISTINCT
    Tenant_Id,
    Template_Name,
    RespondingTeam,
    QuestionType,
    Filter_UpdateTime
from final
{# where Tenant_Id = 1384 #}
