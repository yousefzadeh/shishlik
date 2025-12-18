{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Name] as varchar(200))[Name],
            cast([Detail] as nvarchar(4000)) Detail,
            cast([Tags] as nvarchar(4000)) Tags,
            [Order],
            [PolicyDomainId],
            [TemplateControlId],
            [TenantId],
            [RiskStatus],
            cast([Reference] as nvarchar(4000)) Reference,
            Coalesce([ParentControlId], [Id]) as [ParentControlId],
            Coalesce([RootControlId], [Id]) as [RootControlId],
            case
                when
                    lead([CreationTime]) over (partition by coalesce([RootControlId], [Id]) order by [CreationTime])
                    is null
                then 1
                else 0
            end IsCurrent,
            cast(coalesce(LastModificationTime,CreationTime) as datetime2) as UpdateTime
        from {{ source("assessment_models", "Controls") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Controls") }},
    {{ col_rename("Name", "Controls") }},
    coalesce(
    LastModificationTime, CreationTime
    ) as Controls_LastModificationTime,
    {{ col_rename("Detail", "Controls") }},
    {{ col_rename("Tags", "Controls") }},
    {{ col_rename("Order", "Controls") }},
    {{ col_rename("PolicyDomainId", "Controls") }},

    {{ col_rename("TemplateControlId", "Controls") }},
    {{ col_rename("TenantId", "Controls") }},
    {{ col_rename("RiskStatus", "Controls") }},
    {{ col_rename("Reference", "Controls") }},
    {{ col_rename("ParentControlId", "Controls") }},
    {{ col_rename("RootControlId", "Controls") }},
    {{ col_rename("IsCurrent", "Controls") }},
    {{ col_rename("UpdateTime", "Controls") }}
from base
