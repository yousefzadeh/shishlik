{{ config(materialized="view") }}
with
    base as (
        select DISTINCT
            {{ system_fields_macro() }},
            [TenantId],
            cast([Name] as varchar(200)) as [Name],
            cast([ReferenceId] as varchar(200)) as [ReferenceId],
            cast([Description] as nvarchar(4000)) Description,
            cast([URL] as nvarchar(4000)) URL,
            [AuthorityId],
            [CustomDataJson],
            [Order],
            cast(coalesce(LastModificationTime, CreationTime) as datetime2) UpdateTime,
            coalesce(js.CustomFieldId,0) as CustomFieldId,--replacing nulls to avoid constraint duplication
            js.CustomFieldName,
            js.CustomValueTypeId,
            js.CustomValue
        from {{ source("assessment_models", "AuthorityProvision") }}
        outer apply openjson(CustomDataJson) WITH (  
                    CustomFieldId Int             '$.Id',
                    CustomFieldName nvarchar(max)  '$.Name',
                    CustomValueTypeId Int         '$.FieldTypeId',
                    CustomValue    nvarchar(max)    '$.Value'
                    ) as js
        WHERE IsDeleted = 0
        and ISJSON(CustomDataJson) > 0
    )

select
    {{ col_rename("Id", "AuthorityProvision") }},
    {{ col_rename("TenantId", "AuthorityProvision") }},
    {{ col_rename("Name", "AuthorityProvision") }},
    {{ col_rename("ReferenceId", "AuthorityProvision") }},

    {{ col_rename("Description", "AuthorityProvision") }},
    {{ col_rename("URL", "AuthorityProvision") }},
    {{ col_rename("AuthorityId", "AuthorityProvision") }},
    {{ col_rename("CustomDataJson", "AuthorityProvision") }},

    {{ col_rename("Order", "AuthorityProvision") }},
    {{ col_rename("UpdateTime", "AuthorityProvision") }},
    {{ col_rename("CustomFieldId", "AuthorityProvision") }},
    {{ col_rename("CustomFieldName", "AuthorityProvision") }},

    {{ col_rename("CustomValueTypeId", "AuthorityProvision") }},
    {{ col_rename("CustomValue", "AuthorityProvision") }},
	rank() OVER (ORDER BY Id,TenantId,CustomFieldId) as AuthorityProvision_pk
from base
