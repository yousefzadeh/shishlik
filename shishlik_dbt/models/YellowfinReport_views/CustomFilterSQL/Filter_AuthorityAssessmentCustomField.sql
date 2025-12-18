{# 
DOC START
  - name: Filter_AuthorityAssessmentCustomField
    description: |
        This view is used in a custom SQL filter for the following View:
        - Source Authority To RBA View
               
        It lists One row per 
        - Authority 
        - Template
        - Assessment 
        - Assessment Custom Field
        - Assessment WorkFlow (0 QBA, 1 RBA)

    columns:
      - name: Tenant_Id
        description: ID of Login Tenant for Access Filtering
      - name: SourceAuthority_Name
        description: Name of the Authority
      - name: Template_Name
        description: Name of the Assessment Template 
      - name: Assessment_Name
        description: Name of the Assessment
      - name: CustomField_Name
        description: Name of the Assessment Custom Field
      - name: Assessment_WorkFlowId
        description: Code of the Assessment WorkFlow (0 QBA, 1 RBA)

DOC END    
#}

with final as (
    select distinct
    ap.Tenant_Id,
    a.Authority_Name SourceAuthority_Name,
    t.Assessment_Name Template_Name,
    ass.Assessment_Name Assessment_Name,
    cf.CustomField_Name,
    ass.Assessment_WorkFlowId
    from {{ ref("vwAuthority") }} as a
    inner join {{ ref("vwDirectAuthorityProvision") }} as ap on ap.Authority_Id = a.Authority_Id
    join {{ ref("vwAssessment") }} as ass on ass.Assessment_IsTemplate = 0 and a.Authority_Id = ass.Assessment_AuthorityId and ap.Tenant_Id = ass.Assessment_TenantId
    left join {{ ref("vwAssessment") }} as t on t.Assessment_IsTemplate = 1 and t.Assessment_Id = ass.Assessment_CreatedFromTemplateId and t.Assessment_TenantId = ass.Assessment_TenantId 
    left join {{ ref("vwAssessmentCustomField") }} as acf on ass.Assessment_ID = acf.AssessmentCustomField_AssessmentId
    left join {{ ref("vwCustomField") }} as cf on acf.AssessmentCustomField_CustomFieldId = cf.CustomField_ID
    where ass.Assessment_IsDeleted = 0
    and ass.Assessment_IsArchived = 0
    and ass.Assessment_IsDeprecatedAssessmentVersion = 0
    and ass.Assessment_Status in (4, 5, 6)
)
select 
  Tenant_Id,
  SourceAuthority_Name,
  Template_Name,
  Assessment_Name,
  CustomField_Name,
  Assessment_WorkFlowId
from final
{# 
all tenants 1019 rows, 8s 
where Tenant_Id = 1384 -- 31 rows, <1s
#}

