{{ config(materialized="view") }}

with
    base as (
        select [Id], cast([Name] as nvarchar(4000))[Name], cast([TemplateId] as nvarchar(4000)) TemplateId
        from {{ source("emailtemplate_models", "EmailTemplate") }}
    )

select
    {{ col_rename("Id", "EmailTemplate") }},
    {{ col_rename("Name", "EmailTemplate") }},
    {{ col_rename("TemplateId", "EmailTemplate") }}
from base
