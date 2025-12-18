{{ config(materialized="view") }}
{#
DOC START
  - name: vwAbpEntityPropertyChanges
    description: |
      This view is used to get the list of changes made to the entities in the system.
      This view is used by the AbpEntityChangeSets table.
    columns:
      - name: Id
        description: PK of AbpEntityPropertyChanges table
      - name: EntityChangeId
        description: FK to AbpEntityChanges table
      - name: NewValue
        description: New value of the property
      - name: OriginalValue
        description: Original value of the property
      - name: PropertyName
        description: Name of the property
      - name: PropertyTypeFullName
        description: Type of the property
      - name: PropertyNameVarChar
        description: Name of the property casted to varchar(200)
    
DOC END
#}
with
    base as (
        select [Id], [EntityChangeId], [NewValue], [OriginalValue], [PropertyName], [PropertyTypeFullName], [PropertyNameVarChar], [TenantId]
        from {{ source("assessment_models", "AbpEntityPropertyChanges") }}
    )

select
    {{ col_rename("Id", "AbpEntityPropertyChanges") }},
    {{ col_rename("EntityChangeId", "AbpEntityPropertyChanges") }},
    {{ col_rename("NewValue", "AbpEntityPropertyChanges") }},
    {{ col_rename("OriginalValue", "AbpEntityPropertyChanges") }},
    {{ col_rename("PropertyName", "AbpEntityPropertyChanges") }},
    {{ col_rename("PropertyTypeFullName", "AbpEntityPropertyChanges") }},
    {{ col_rename("PropertyNameVarChar", "AbpEntityPropertyChanges") }},
    {{ col_rename("TenantId", "AbpEntityPropertyChanges") }}
from base
