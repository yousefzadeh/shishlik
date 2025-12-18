{{ config(materialized="view") }}

with base as (select [Id], cast([Name] as nvarchar(4000))[Name] from {{ source("industry_models", "Industry") }})

select {{ col_rename("Id", "Industry") }}, {{ col_rename("Name", "Industry") }}
from base
