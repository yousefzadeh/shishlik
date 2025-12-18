{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [IssueId],
            [ThirdPartyControlId],
            cast([TextData] as nvarchar(4000)) TextData,
            [CustomDateValue],
            [NumberValue],
            CONCAT(IssueId, ThirdPartyControlId) as PK
        from {{ source("issue_models", "IssueFreeTextControlData") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "IssueFreeTextControlData") }},
    {{ col_rename("IssueId", "IssueFreeTextControlData") }},
    {{ col_rename("ThirdPartyControlId", "IssueFreeTextControlData") }},
    {{ col_rename("TextData", "IssueFreeTextControlData") }},

    {{ col_rename("CustomDateValue", "IssueFreeTextControlData") }},
    {{ col_rename("NumberValue", "IssueFreeTextControlData") }},
    {{ col_rename("PK", "IssueFreeTextControlData") }}
from
    base
