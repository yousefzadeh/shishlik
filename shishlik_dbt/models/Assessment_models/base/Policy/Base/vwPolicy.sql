{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Name] as nvarchar(4000)) Name,
            cast([Description] as nvarchar(4000)) Description,
            cast([Tags] as nvarchar(4000)) Tags,
            cast([SupplierName] as nvarchar(4000)) SupplierName,
            [Status],
            case
                when [Status] = 1
                then 'Edit'
                when [Status] = 2
                then 'Published'
                when [Status] = 100
                then 'Deprecated'
                else 'Undefined'
            end as [StatusCode],
            cast([Type] as nvarchar(4000))[Type],
            [LastReviewDate],
            [NextReviewDate],
            [IsTemplate],
            [TemplatedId],
            COALESCE([ParentPolicyId], [Id]) as [ParentPolicyId],
            COALESCE([RootPolicyId], [Id]) as [RootPolicyId],
            [Version],
            'v'
            + cast([Version] as varchar(13))
            + ' ('
            + coalesce(
                cast(format([PublishedDate], 'dd MMM, yyyy') as varchar),
                cast(format(getdate(), 'dd MMM, yyyy') as varchar)
            )
            + ')' VersionDate,
            [PublishedDate],
            case when [PublishedDate] is NULL then 0 else 1 end as [IsPublished],
            [PublishedById],
            cast([ImageUrl] as nvarchar(4000)) ImageUrl,
            [HideResponsibilityTasksUntilRepublished],
            [LastPublishedDate],
            {{ IsCurrentRow("RootPolicyId") }},
             cast(coalesce([LastModificationTime],[CreationTime]) as datetime2) as UpdateTime
        from {{ source("assessment_models", "Policy") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Policy") }},
    {{ col_rename("TenantId", "Policy") }},
    {{ col_rename("Name", "Policy") }},
    {{ col_rename("Description", "Policy") }},

    {{ col_rename("Tags", "Policy") }},
    {{ col_rename("SupplierName", "Policy") }},
    {{ col_rename("Status", "Policy") }},
    {{ col_rename("StatusCode", "Policy") }},
    {{ col_rename("Type", "Policy") }},

    {{ col_rename("LastReviewDate", "Policy") }},
    {{ col_rename("NextReviewDate", "Policy") }},
    {{ col_rename("IsTemplate", "Policy") }},
    {{ col_rename("TemplatedId", "Policy") }},

    {{ col_rename("ParentPolicyId", "Policy") }},
    {{ col_rename("RootPolicyId", "Policy") }},
    {{ col_rename("Version", "Policy") }},
    {{ col_rename("IsPublished", "Policy") }},
    {{ col_rename("PublishedDate", "Policy") }},
    {{ col_rename("VersionDate", "Policy") }},

    {{ col_rename("PublishedById", "Policy") }},
    {{ col_rename("ImageUrl", "Policy") }},
    {{ col_rename("HideResponsibilityTasksUntilRepublished", "Policy") }},
    {{ col_rename("LastPublishedDate", "Policy") }},
    {{ col_rename("IsCurrent", "Policy") }},
    {{ col_rename("UpdateTime", "Policy") }}
from base
