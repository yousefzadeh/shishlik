{#  
DOC START
  - name: Filter_TemplateAssessmentCustomField
    description: |
        This view is used in a custom SQL filter for the following View:
        - Requirements Based Assessments
               
        For RBA, in the set up for creation of assessments, Responding teams will be selected to respond to the assessment. 
        In the Third Party Management screen to setup the Responding Team (Vendor), the Custom Fields are defined with associated Values.
        In addition to that, there are Custom Field and Value defined for Assessments.

        It lists One row per 
        - RBAssessment
        - Custom Field 

    columns:
      - name: Assessment_Name
        description: Name of the RBAssessment
      - name: CustomField_Name
        description: Name of the Custom Field defined for the Assessment 

DOC END    
#}
with final as (
    SELECT DISTINCT
        ass.Assessment_TenantId Tenant_Id,
        asst.Assessment_Name Template_Name,
        ass.Assessment_Name Assessment_Name,
        ass.Assessment_Name_Responding_Team AssessmentRespondingTeam_Name,
        cf.CustomField_Name,
        ass.Assessment_QuestionType QuestionType,
        max(GREATEST(cast(asst.Assessment_UpdateTime as datetime2), cast(ass.Assessment_UpdateTime as datetime2), ISNUll(cast(cf.CustomField_UpdateTime as datetime2), '2000-01-01 00:00:00'))) over (partition by ass.Assessment_TenantId,asst.Assessment_Name,ass.Assessment_Name, ass.Assessment_Name_Responding_Team, ass.Assessment_QuestionType, isnull(cf.CustomField_Name,''))  AS Filter_UpdateTime
    FROM {{ ref("vwAssessment") }} as ass 
    left join {{ ref("vwAssessment") }} as asst on asst.Assessment_IsTemplate = 1 and asst.Assessment_Id = ass.Assessment_CreatedFromTemplateId and asst.Assessment_TenantId = ass.Assessment_TenantId 
    left join {{ ref("vwAssessmentCustomField") }} as acf on ass.Assessment_ID = acf.AssessmentCustomField_AssessmentId
    left join {{ ref("vwCustomField") }} as cf on acf.AssessmentCustomField_CustomFieldId = cf.CustomField_ID
    where ass.Assessment_IsDeleted = 0
    and ass.Assessment_IsArchived = 0
    and ass.Assessment_IsDeprecatedAssessmentVersion = 0
    and ass.Assessment_Status in (4, 5, 6)
) 
select 
Tenant_Id,
Template_Name,
Assessment_Name,
AssessmentRespondingTeam_Name,
CustomField_Name,
QuestionType,
Filter_UpdateTime
from final
{# where Tenant_Id IN (1384) #}
