{{ config(materialized="view") }}

with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [TenantId],
            cast([RiskName] as nvarchar(4000)) RiskName,
            cast([RiskDescription] as nvarchar(4000)) RiskDescription,
            -- ,cast([CurrentLikelihood] as nvarchar(4000)) CurrentLikelihood
            -- ,cast([CurrentConsequence] as nvarchar(4000)) CurrentConsequence
            -- ,cast([AssociatedControls] as nvarchar(4000)) AssociatedControls
            -- ,cast([AssociatedProvisions] as nvarchar(4000)) AssociatedProvisions
            cast([RiskOwners] as nvarchar(4000)) RiskOwners,
            cast([AssociatedAssets] as nvarchar(4000)) AssociatedAssets,
            cast([AssociatedTags] as nvarchar(4000)) AssociatedTags,
            cast([TreatmentDecision] as nvarchar(4000)) TreatmentDecision,
            cast([TreatmentStatus] as nvarchar(4000)) TreatmentStatus,
            cast([TreatmentDescription] as nvarchar(4000)) TreatmentDescription,
            -- ,cast([PlannedLikelihood] as nvarchar(4000)) PlannedLikelihood
            -- ,cast([PlannedConsequence] as nvarchar(4000)) PlannedConsequence
            -- ,cast([TreatmentAssociatedControls] as nvarchar(4000)) TreatmentAssociatedControls
            -- ,cast([TreatmentAssociatedProvisions] as nvarchar(4000)) TreatmentAssociatedProvisions
            cast([TreatmentOwners] as nvarchar(4000)) TreatmentOwners,
            cast([TreatmentDate] as nvarchar(4000)) TreatmentDate,
            [HasError],
            cast([ErrorMessage] as nvarchar(4000)) ErrorMessage,
            [IsExists],
            [IsDuplicate],
            [IsImported],
            [ImportFileLogId],
            cast([TreatmentPlanName] as nvarchar(4000)) TreatmentPlanName,
            cast([TreatmentPlanStatus] as nvarchar(4000)) TreatmentPlanStatus,
            cast([CommonCauses] as nvarchar(4000)) CommonCauses,
            cast([LikelyImpacts] as nvarchar(4000)) LikelyImpacts,
            cast([IdentifiedBy] as nvarchar(4000)) IdentifiedBy,
            cast([CustomData] as nvarchar(4000)) CustomData
        from {{ source("risk_models", "RiskImportStagingRecord") }}

    )

select
    {{ col_rename("Id", "RiskImportStagingRecord") }},
    {{ col_rename("CreationTime", "RiskImportStagingRecord") }},
    {{ col_rename("CreatorUserId", "RiskImportStagingRecord") }},
    {{ col_rename("TenantId", "RiskImportStagingRecord") }},

    {{ col_rename("RiskName", "RiskImportStagingRecord") }},
    {{ col_rename("RiskDescription", "RiskImportStagingRecord") }},
    -- {{ col_rename('CurrentLikelihood','RiskImportStagingRecord')}},
    -- {{ col_rename('CurrentConsequence','RiskImportStagingRecord')}},
    -- {{ col_rename('AssociatedControls','RiskImportStagingRecord')}},
    -- {{ col_rename('AssociatedProvisions','RiskImportStagingRecord')}},
    {{ col_rename("RiskOwners", "RiskImportStagingRecord") }},
    {{ col_rename("AssociatedAssets", "RiskImportStagingRecord") }},

    {{ col_rename("AssociatedTags", "RiskImportStagingRecord") }},
    {{ col_rename("TreatmentDecision", "RiskImportStagingRecord") }},
    {{ col_rename("TreatmentStatus", "RiskImportStagingRecord") }},
    {{ col_rename("TreatmentDescription", "RiskImportStagingRecord") }},

    -- {{ col_rename('PlannedLikelihood','RiskImportStagingRecord')}},
    -- {{ col_rename('PlannedConsequence','RiskImportStagingRecord')}},      
    -- {{ col_rename('TreatmentAssociatedControls','RiskImportStagingRecord')}},
    -- {{ col_rename('TreatmentAssociatedProvisions','RiskImportStagingRecord')}},
    {{ col_rename("TreatmentOwners", "RiskImportStagingRecord") }},
    {{ col_rename("TreatmentDate", "RiskImportStagingRecord") }},
    {{ col_rename("HasError", "RiskImportStagingRecord") }},
    {{ col_rename("ErrorMessage", "RiskImportStagingRecord") }},

    {{ col_rename("IsExists", "RiskImportStagingRecord") }},
    {{ col_rename("IsDuplicate", "RiskImportStagingRecord") }},
    {{ col_rename("IsImported", "RiskImportStagingRecord") }},
    {{ col_rename("ImportFileLogId", "RiskImportStagingRecord") }},

    {{ col_rename("TreatmentPlanName", "RiskImportStagingRecord") }},
    {{ col_rename("TreatmentPlanStatus", "RiskImportStagingRecord") }},
    {{ col_rename("CommonCauses", "RiskImportStagingRecord") }},
    {{ col_rename("LikelyImpacts", "RiskImportStagingRecord") }},
    {{ col_rename("IdentifiedBy", "RiskImportStagingRecord") }},

    {{ col_rename("CustomData", "RiskImportStagingRecord") }}
from base
