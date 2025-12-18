{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([StakeHolderName] as nvarchar(4000)) StakeHolderName,
            [Role],
            case
                [Role]
                when 1
                then 'Owner'
                when 2
                then 'Reviewers'
                when 3
                then 'Readers'
                when 4
                then 'Approvers'
            end RoleCode,
            [PolicyId],
            [UserId],
            [OrganizationUnitId]
        from
            {{ source("assessment_models", "PolicyStakeHolders") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "PolicyStakeHolders") }},
    {{ col_rename("CreationTime", "PolicyStakeHolders") }},
    {{ col_rename("LastModificationTime", "PolicyStakeHolders") }},
    {{ col_rename("TenantId", "PolicyStakeHolders") }},
    {{ col_rename("StakeHolderName", "PolicyStakeHolders") }},
    {{ col_rename("Role", "PolicyStakeHolders") }},
    {{ col_rename("RoleCode", "PolicyStakeHolders") }},
    {{ col_rename("PolicyId", "PolicyStakeHolders") }},
    {{ col_rename("UserId", "PolicyStakeHolders") }},
    {{ col_rename("OrganizationUnitId", "PolicyStakeHolders") }}
from base
