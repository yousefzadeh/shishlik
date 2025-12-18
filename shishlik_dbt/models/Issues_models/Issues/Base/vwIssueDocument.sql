{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([DocumentFileName] as nvarchar(4000)) DocumentFileName,
            cast([DisplayFileName] as nvarchar(4000)) DisplayFileName,
            cast([DocumentUrl] as nvarchar(4000)) DocumentUrl,
            [FileSizeInKB],
            [IssueId]
        from {{ source("issue_models", "IssueDocument") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "IssueDocument") }},
    {{ col_rename("TenantId", "IssueDocument") }},
    {{ col_rename("DocumentFileName", "IssueDocument") }},
    {{ col_rename("DisplayFileName", "IssueDocument") }},

    {{ col_rename("DocumentUrl", "IssueDocument") }},
    {{ col_rename("FileSizeInKB", "IssueDocument") }},
    {{ col_rename("IssueId", "IssueDocument") }}
from
    base

    /*One issue can have multiple documents. 

SELECT
    --     [IssueDocument_Id]
    --   ,[IssueDocument_TenantId]
    --   ,[IssueDocument_DocumentFileName]
    --   ,[IssueDocument_DisplayFileName]
    --   ,[IssueDocument_DocumentUrl]
    --   ,[IssueDocument_FileSizeInKB]
      [IssueDocument_IssueId]
      ,count(*)
  FROM [dev_naunghton_williams].[vwIssueDocument]
  GROUP BY
   --     [IssueDocument_Id]
    --   ,[IssueDocument_TenantId]
    --   ,[IssueDocument_DocumentFileName]
    --   ,[IssueDocument_DisplayFileName]
    --   ,[IssueDocument_DocumentUrl]
    --   ,[IssueDocument_FileSizeInKB]
      [IssueDocument_IssueId]
ORDER BY COUNT(*) desc


*/
    
