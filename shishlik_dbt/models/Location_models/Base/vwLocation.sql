{{ config(materialized="view") }}

with
    base as (
        select [Id], cast([Name] as nvarchar(4000))[Name], [SignupLocation]
        from {{ source("location_models", "Location") }}
    )

select
    {{ col_rename("Id", "Location") }},
    {{ col_rename("Name", "Location") }},
    {{ col_rename("SignupLocation", "Location") }}
from base
