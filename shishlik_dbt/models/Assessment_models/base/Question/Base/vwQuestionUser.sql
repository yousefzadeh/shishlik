{{ config(materialized="view") }}


{# 
DOC START
  - name: vwQuestionUser
    description: |
      This table Associates an assigned user review to each Question, QuestionGroup and QuestionGroupResponse.
      One row per Question, or QuestionGroup or QuestionGroupResponse per Assigned Reviewer User or OrganizationUnit.

    columns:
      - name: QuestionUser_ID
        description: PK of QuestionUser table
      - name: QuestionUser_QuestionGroupId
        description: FK to QuestionGroup table (Nullable)
      - name: QuestionUser_QuestionGroupResponseId
        description: FK to QuestionGroupResponse table (Nullable)
      - name: QuestionUser_QuestionId
        description: FK to Question table (Nullable)
      - name: QuestionUser_UserId
        description: FK to AbpUsers table (Nullable)
      - name: QuestionUser_OrganizationUnitId
        description: FK to AbpOrganizationUnits table (Nullable)
      - name: QuestionUser_TenantId

DOC END    
#}

with
    base as (
        select {{ system_fields_macro() }}, [QuestionId], [UserId], [TenantId], [OrganizationUnitId], [QuestionGroupId], [QuestionGroupResponseId]
        from {{ source("assessment_models", "QuestionUser") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "QuestionUser") }},
    {{ col_rename("QuestionGroupId", "QuestionUser") }},
    {{ col_rename("QuestionGroupResponseId", "QuestionUser") }},
    {{ col_rename("QuestionId", "QuestionUser") }},
    {{ col_rename("CreationTime", "QuestionUser") }},
    {{ col_rename("LastModificationTime", "QuestionUser") }},
    {{ col_rename("UserId", "QuestionUser") }},
    {{ col_rename("OrganizationUnitId", "QuestionUser") }},
    {{ col_rename("TenantId", "QuestionUser") }}
from base
