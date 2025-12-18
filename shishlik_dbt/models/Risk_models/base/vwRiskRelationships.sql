with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            RiskId,
            RelatedRiskId,
            case 
            when RelationshipType = 1 then 'Parent'
            when RelationshipType = 2 then 'Child'
            when RelationshipType = 3 then 'Related'
            end RelationshipType
        from {{ source("risk_models", "RiskRelationships") }} {{ system_remove_IsDeleted() }}
    )
 
select
    {{ col_rename("Id", "RiskRelationships") }},
    {{ col_rename("CreationTime", "RiskRelationships") }},
    {{ col_rename("CreatorUserId", "RiskRelationships") }},
    {{ col_rename("LastModificationTime", "RiskRelationships") }},
 
    {{ col_rename("LastModifierUserId", "RiskRelationships") }},
    {{ col_rename("IsDeleted", "RiskRelationships") }},
    {{ col_rename("DeleterUserId", "RiskRelationships") }},
    {{ col_rename("DeletionTime", "RiskRelationships") }},
 
    {{ col_rename("TenantId", "RiskRelationships") }},
    {{ col_rename("RiskId", "RiskRelationships") }},
    {{ col_rename("RelatedRiskId", "RiskRelationships") }},
    {{ col_rename("RelationshipType", "RiskRelationships") }}
from base