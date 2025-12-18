{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [SourceAuthorityId],
            [TargetAuthorityId],
            cast([RequestMessage] as nvarchar(4000)) RequestMessage,
            cast([RequestId] as nvarchar(4000)) RequestId,
            [RequestStatus],
            [RequestedTenantId],
            [RequestedUserId]
        from {{ source("assessment_models", "AuthoritySimilarityComparisonRequest") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AuthoritySimilarityComparisonRequest") }},
    {{ col_rename("CreationTime", "AuthoritySimilarityComparisonRequest") }},
    {{ col_rename("CreatorUserId", "AuthoritySimilarityComparisonRequest") }},
    {{ col_rename("LastModificationTime", "AuthoritySimilarityComparisonRequest") }},

    {{ col_rename("SourceAuthorityId", "AuthoritySimilarityComparisonRequest") }},
    {{ col_rename("TargetAuthorityId", "AuthoritySimilarityComparisonRequest") }},
    {{ col_rename("RequestMessage", "AuthoritySimilarityComparisonRequest") }},
    {{ col_rename("RequestId", "AuthoritySimilarityComparisonRequest") }},

    {{ col_rename("RequestStatus", "AuthoritySimilarityComparisonRequest") }},
    {{ col_rename("RequestedTenantId", "AuthoritySimilarityComparisonRequest") }},
    {{ col_rename("RequestedUserId", "AuthoritySimilarityComparisonRequest") }},
    {{ col_rename("DeleterUserId", "AuthoritySimilarityComparisonRequest") }},

    {{ col_rename("DeletionTime", "AuthoritySimilarityComparisonRequest") }},
    {{ col_rename("IsDeleted", "AuthoritySimilarityComparisonRequest") }},
    {{ col_rename("LastModifierUserId", "AuthoritySimilarityComparisonRequest") }}
from base
