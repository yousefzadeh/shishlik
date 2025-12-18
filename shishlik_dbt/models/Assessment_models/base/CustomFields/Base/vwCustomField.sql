{{ config(materialized="view") }}

/*
Enums for Datatype column:
CustomFieldDataType
    {
        Dropdown = 1,

        [Description("Short Text Response")]
        ShortTextResponse = 2,

 

        [Description("Long Text Response")]
        LongTextResponse = 3
    }
*/
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Name] as varchar(200))[Name],
            cast([Description] as nvarchar(4000)) Description,
            [DataType],
            CASE [DataType] 
            WHEN 1 then 'Drop Down' 
            WHEN 2 then 'Short Text Response' 
            WHEN 3 then 'Long Text Response' 
            END as DataTypeCode,
            coalesce(LastModificationTime, CreationTime) UpdateTime
        from {{ source("assessment_models", "CustomField") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "CustomField") }},
    {{ col_rename("TenantId", "CustomField") }},
    {{ col_rename("Name", "CustomField") }},
    {{ col_rename("Description", "CustomField") }},
    {{ col_rename("DataType", "CustomField") }},
    {{ col_rename("DataTypeCode", "CustomField") }},
    {{ col_rename("UpdateTime", "CustomField") }}
from base
