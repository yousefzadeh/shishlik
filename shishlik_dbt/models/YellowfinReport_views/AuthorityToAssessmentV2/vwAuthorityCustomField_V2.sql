{{ config(materialized="view") }}
with
    base as (
        select Id, [AuthorityId], cast([FieldName] as varchar(200))[FieldName]
        from {{ source("assessment_models", "AuthorityProvisionCustomField") }}
    )

select
    {{ col_rename("Id", "AuthorityCustomField") }},
    {{ col_rename("AuthorityId", "AuthorityCustomField") }},
    {{ col_rename("FieldName", "AuthorityCustomField") }}
from base
