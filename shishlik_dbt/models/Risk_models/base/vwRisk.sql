{{ config(materialized="view") }}

with
    base as (
        SELECT
            r.Id,
            r.CreationTime,
            r.CreatorUserId,
            r.LastModificationTime,
            r.LastModifierUserId,
            CAST([Name] AS VARCHAR(500)) AS [Name],
            CAST([Description] AS NVARCHAR(4000)) AS Description,
            r.TenantId AS TenantId,
            at2.AbpTenants_Name AS AbpTenants_Name,
            [Status],
            CASE
                WHEN [Status] = 1 THEN 'Edit'
                WHEN [Status] = 2 THEN 'Published'
                WHEN [Status] = 100 THEN 'Deprecated'
                ELSE 'Undefined'
            END AS [StatusCode],
            [AbstractRiskId],
            [RiskReviewId],
            CAST([CommonCause] AS NVARCHAR(4000)) AS CommonCause,
            CAST([LikelyImpact] AS NVARCHAR(4000)) AS LikelyImpact,
            [IdentifiedBy],
            [FavouriteRiskAssessmentId],
            CAST([TenantEntityUniqueId] AS VARCHAR) AS IdRef,
            [WorkflowStageId],
            [TreatmentDecisionId],
            [TreatmentStatusId],
            CASE
                WHEN [TreatmentStatusId] = 1 THEN 'Draft'
                WHEN [TreatmentStatusId] = 2 THEN 'Approved'
                WHEN [TreatmentStatusId] = 3 THEN 'Treatment in progress'
                WHEN [TreatmentStatusId] = 4 THEN 'Treatment paused'
                WHEN [TreatmentStatusId] = 5 THEN 'Treatment cancelled'
                WHEN [TreatmentStatusId] = 6 THEN 'Treatment completed'
                WHEN [TreatmentStatusId] = 7 THEN 'Closed'
                ELSE 'Undefined'
            END AS [TreatmentStatusCode],
            COALESCE(r.LastModificationTime, r.CreationTime) AS SnapshotDate,
            RiskRatingId,
            tpa.ThirdPartyAttributes_Label AS Rating,
            NULL AS NULL_Risk,
            r.IsArchived,
            0 AS IsShared,
            CAST(COALESCE(r.LastModificationTime, r.CreationTime) AS DATETIME2) AS UpdateTime
        FROM {{ source("risk_models", "Risk") }} r
        JOIN {{ ref("vwAbpTenants") }} at2 ON at2.AbpTenants_Id = r.TenantId
        LEFT JOIN {{ ref("vwThirdPartyAttributes") }} tpa ON tpa.ThirdPartyAttributes_Id = r.RiskRatingId
        WHERE r.IsDeleted = 0

        UNION 

        -- Shared risks
        SELECT
            r.Id,
            r.CreationTime,
            r.CreatorUserId,
            r.LastModificationTime,
            r.LastModifierUserId,
            CAST([Name] AS VARCHAR(500)) AS [Name],
            CAST([Description] AS NVARCHAR(4000)) AS Description,
            e.DestinationTenantId AS TenantId,
            abp2.AbpTenants_Name AS AbpTenants_Name,
            [Status],
            CASE
                WHEN [Status] = 1 THEN 'Edit'
                WHEN [Status] = 2 THEN 'Published'
                WHEN [Status] = 100 THEN 'Deprecated'
                ELSE 'Undefined'
            END AS [StatusCode],
            [AbstractRiskId],
            [RiskReviewId],
            CAST([CommonCause] AS NVARCHAR(4000)) AS CommonCause,
            CAST([LikelyImpact] AS NVARCHAR(4000)) AS LikelyImpact,
            [IdentifiedBy],
            [FavouriteRiskAssessmentId],
            CAST([TenantEntityUniqueId] AS VARCHAR) AS IdRef,
            [WorkflowStageId],
            [TreatmentDecisionId],
            [TreatmentStatusId],
            CASE
                WHEN [TreatmentStatusId] = 1 THEN 'Draft'
                WHEN [TreatmentStatusId] = 2 THEN 'Approved'
                WHEN [TreatmentStatusId] = 3 THEN 'Treatment in progress'
                WHEN [TreatmentStatusId] = 4 THEN 'Treatment paused'
                WHEN [TreatmentStatusId] = 5 THEN 'Treatment cancelled'
                WHEN [TreatmentStatusId] = 6 THEN 'Treatment completed'
                WHEN [TreatmentStatusId] = 7 THEN 'Closed'
                ELSE 'Undefined'
            END AS [TreatmentStatusCode],
            COALESCE(r.LastModificationTime, r.CreationTime) AS SnapshotDate,
            RiskRatingId,
            tpa.ThirdPartyAttributes_Label AS Rating,
            NULL AS NULL_Risk,
            r.IsArchived,
            1 AS IsShared,
            CAST(COALESCE(r.LastModificationTime, r.CreationTime) AS DATETIME2) AS UpdateTime
        FROM {{ source("risk_models", "Risk") }} r
        LEFT JOIN {{ source("risk_models", "EntityShareLog") }} e
            ON e.TenantId = r.TenantId
        AND e.SourceEntityId = r.Id
        AND e.IsDeleted = 0
        LEFT JOIN {{ ref("vwAbpTenants") }} abp2 ON abp2.AbpTenants_Id = e.DestinationTenantId
        LEFT JOIN {{ ref("vwThirdPartyAttributes") }} tpa ON tpa.ThirdPartyAttributes_Id = r.RiskRatingId
        WHERE r.IsDeleted = 0
        AND e.DestinationTenantId IS NOT NULL
    )

select
    {{ col_rename("Id", "Risk") }},
    {{ col_rename("TenantId", "Risk") }},
    {{ col_rename("AbpTenants_Name", "Risk") }},
    {{ col_rename("Name", "Risk") }},
    {{ col_rename("Description", "Risk") }},

    {{ col_rename("Status", "Risk") }},
    {{ col_rename("IdRef", "Risk") }},
    {{ col_rename("StatusCode", "Risk") }},  -- derived

    {{ col_rename("AbstractRiskId", "Risk") }},
    {{ col_rename("RiskReviewId", "Risk") }},
    {{ col_rename("CommonCause", "Risk") }},
    {{ col_rename("CreationTime", "Risk") }},
    {{ col_rename("LastModificationTime", "Risk") }},
    {{ col_rename("SnapshotDate", "Risk") }},

    {{ col_rename("LikelyImpact", "Risk") }},
    {{ col_rename("IdentifiedBy", "Risk") }},
    {{ col_rename("FavouriteRiskAssessmentId", "Risk") }},
    {{ col_rename("WorkflowStageId", "Risk") }},
    {{ col_rename("TreatmentDecisionId", "Risk") }},
    {{ col_rename("TreatmentStatusId", "Risk") }},
    {{ col_rename("TreatmentStatusCode", "Risk") }},
    {{ col_rename("RiskRatingId", "Risk") }},
    {{ col_rename("Rating", "Risk") }},
    {{ col_rename("NULL_Risk", "Risk") }},
    {{ col_rename("IsArchived", "Risk") }},
    {{ col_rename("IsShared", "Risk") }},
    {{ col_rename("UpdateTime", "Risk") }}
from base
