{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [AuthorityId],
            cast([FieldName] as varchar(200))[FieldName],
            cast([FieldType] as varchar(200))[FieldType],
            [Order]
        from {{ source("assessment_models", "AuthorityProvisionCustomField") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AuthorityProvisionCustomField") }},
    {{ col_rename("CreationTime", "AuthorityProvisionCustomField") }},
    {{ col_rename("CreatorUserId", "AuthorityProvisionCustomField") }},
    {{ col_rename("LastModificationTime", "AuthorityProvisionCustomField") }},

    {{ col_rename("LastModifierUserId", "AuthorityProvisionCustomField") }},
    {{ col_rename("IsDeleted", "AuthorityProvisionCustomField") }},
    {{ col_rename("DeleterUserId", "AuthorityProvisionCustomField") }},
    {{ col_rename("DeletionTime", "AuthorityProvisionCustomField") }},

    {{ col_rename("AuthorityId", "AuthorityProvisionCustomField") }},
    {{ col_rename("FieldName", "AuthorityProvisionCustomField") }},
    {{ col_rename("FieldType", "AuthorityProvisionCustomField") }},
    {{ col_rename("Order", "AuthorityProvisionCustomField") }}
from base
