with
    source as (
        select
            {{ system_fields_macro() }},
            tenantid,
            comment,
            userid,
            statementresponseid responsibilitytaskid
        from {{ source("statement_models", "StatementResponseComment") }}
    ),
    renamed as (
        select
            {{ col_rename("Id", "ResponsibilityTaskComment") }},
            {{ col_rename("TenantId", "ResponsibilityTaskComment") }},
            {{ col_rename("Comment", "ResponsibilityTaskComment") }},
            {{ col_rename("UserId", "ResponsibilityTaskComment") }},
            {{ col_rename("ResponsibilityTaskId", "ResponsibilityTaskComment") }},
            {{ col_rename("CreationTime", "ResponsibilityTaskComment") }}
        from source
    )
select *
from renamed
