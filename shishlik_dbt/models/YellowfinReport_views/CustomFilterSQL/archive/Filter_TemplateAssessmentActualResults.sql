{# 
DOC START
  - name: Filter_TemplateAssessmentActualResults
    description: |
        This view is used in a custom SQL filter for the following View:
        - Question Based Assessment Answer Details
               
        It lists One row per 
        - Template
        - Question Based Assessment (Status 4,6) 
        - Actual Results
        For Reqular Question Answer as well as Question Group Answer


    columns:
      - name: Tenant_Id
        description: ID of Login Tenant for Access Filtering
      - name: Template_Name
        description: Name of the Assessment Template 
      - name: Assessment_Name
        description: Name of the Assessment
      - name: ActualResults
        description: Actual Results as entered by the responder

DOC END    
#}
with 
    final as (
      select 
      * 
      from {{ ref("Filter_TemplateQBAssessmentActualResults") }}
    )
SELECT 
    Tenant_Id,
    Template_Name,
    Assessment_Name,
    ActualResults
FROM final
{# where Tenant_Id = 1384 #}