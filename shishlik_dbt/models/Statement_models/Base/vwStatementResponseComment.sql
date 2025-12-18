with
    source as (
        select
            {{ system_fields_macro() }}, tenantid, comment, userid, statementresponseid
        from {{ source("statement_models", "StatementResponseComment") }}
    ),
    renamed as (
        select
            {{ col_rename("Id", "StatementResponseComment") }},
            {{ col_rename("TenantId", "StatementResponseComment") }},
            {{ col_rename("Comment", "StatementResponseComment") }},
            {{ col_rename("UserId", "StatementResponseComment") }},
            {{ col_rename("StatementResponseId", "StatementResponseComment") }}
        from source
    )
select *
from renamed
