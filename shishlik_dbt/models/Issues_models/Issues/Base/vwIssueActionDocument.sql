{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([FileName] as nvarchar(4000)) FileName,
            cast([DisplayFileName] as nvarchar(4000)) DisplayFileName,
            cast([ContainerName] as nvarchar(4000)) ContainerName,
            [FileSizeInKB],
            [IssueActionId]
        from {{ source("issue_models", "IssueActionDocument") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "IssueActionDocument") }},
    {{ col_rename("FileName", "IssueActionDocument") }},
    {{ col_rename("DisplayFileName", "IssueActionDocument") }},
    {{ col_rename("ContainerName", "IssueActionDocument") }},

    {{ col_rename("FileSizeInKB", "IssueActionDocument") }},
    {{ col_rename("IssueActionId", "IssueActionDocument") }}
from
    base

    /*Issue Actions can have multiple Documents. Needs to be considered when used for modeling

SELECT IssueActionDocument_IssueActionId
,COUNT(*)
 FROM [dev_naunghton_williams].[vwIssueActionDocument]
 GROUP BY IssueActionDocument_IssueActionId
 ORDER BY COUNT(*) desc
*/
    
