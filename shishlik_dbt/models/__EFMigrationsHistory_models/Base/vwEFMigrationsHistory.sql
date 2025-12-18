{{ config(materialized="view") }}

with
    base as (
        select [MigrationId], [ProductVersion]
        from {{ source("__EFMigrationsHistory_models", "__EFMigrationsHistory") }}
    )

select
    {{ col_rename("MigrationId", "__EFMigrationsHistory") }},
    {{ col_rename("ProductVersion", "__EFMigrationsHistory") }}
from base
