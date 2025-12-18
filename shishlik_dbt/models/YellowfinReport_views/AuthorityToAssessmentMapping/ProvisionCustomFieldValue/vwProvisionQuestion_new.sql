{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [QuestionId],
            [AuthorityProvisionId],
            coalesce(LastModificationTime, CreationTime) UpdateTime
        from {{ source("assessment_models", "ProvisionQuestion") }} {{ system_remove_IsDeleted() }}
    ),
    provision as (
        select
            {{ col_rename("Id", "ProvisionQuestion") }},
            {{ col_rename("TenantId", "ProvisionQuestion") }},
            {{ col_rename("QuestionId", "ProvisionQuestion") }},
            {{ col_rename("AuthorityProvisionId", "ProvisionQuestion") }}
        from base
    ),
    controls as (
        select
            cq.ControlQuestion_Id ProvisionQuestion_Id,
            cq.ControlQuestion_TenantId ProvisionQuestion_TenantId,
            cq.ControlQuestion_QuestionId ProvisionQuestion_QuestionId,
            pc.ProvisionControl_AuthorityReferenceId ProvisionQuestion_AuthorityProvisionId
        from {{ ref("vwControlQuestion") }} cq
        join {{ ref("vwProvisionControl") }} pc on cq.ControlQuestion_ControlsId = pc.ProvisionControl_ControlsId
    ),
    final as (
        select *
        from provision
        union all
        select *
        from controls
    )
select *
from final
