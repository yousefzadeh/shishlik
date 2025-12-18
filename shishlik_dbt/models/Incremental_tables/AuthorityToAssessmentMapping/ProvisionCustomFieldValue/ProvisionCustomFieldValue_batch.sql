{{-
    config(
        {
            "materialized": "incremental",
            "tags": ["nightly"],
            "as_columnstore": false,
            "unique_key": "ProvisionCustomFieldValue_PK",
            "pre-hook": "{{ incremental_drop_all_indexes_on_table() }}",
            "post-hook": [
                "{{ incremental_create_clustered_index(columns = ['ProvisionCustomFieldValue_PK']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['AuthorityCustom_AuthorityId']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['AuthorityCustom_FieldName']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['Provision_Id']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['ProvisionCustom_Value']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['ProvisionCustomFieldValue_UpdateTime']) }}",
            ],
        }
    )
-}}
select *
from {{ ref("vwProvisionCustomFieldValue_source") }}
{% if is_incremental() -%}
    -- -- Incremental run
    where ProvisionCustomFieldValue_UpdateTime > (select max(ProvisionCustomFieldValue_UpdateTime) from {{ this }})
{% else -%}
    -- -- Full run
    where ProvisionCustomFieldValue_UpdateTime < '2022-07-01 00:00:00.000'  {# fake full run so incremental will have rows to add #}
{%- endif -%}
