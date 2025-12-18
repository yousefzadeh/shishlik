{{-
    config(
        {
            "materialized": "incremental",
            "tags": ["nightly"],
            "as_columnstore": false,
            "unique_key": "AnswerResponse_PK",
            "pre-hook": "{{ incremental_drop_all_indexes_on_table() }}",
            "post-hook": [
                "{{ incremental_create_clustered_index(columns = ['Answer_Id']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['AnswerResponse_UpdateTime']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['Question_ID']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['AnswerResponse_Value']) }}",
            ],
        }
    )
-}}
select *
from {{ ref("vwAnswerResponseChooseMany_source") }}
{% if is_incremental() -%}
    -- -- Incremental run
    where AnswerResponse_UpdateTime > (select max(AnswerResponse_UpdateTime) from {{ this }})
{% else -%}
    -- -- Full run
    where AnswerResponse_UpdateTime < '2022-07-01 00:00:00.000'  {# fake full run so incremental will have rows to add #}
{%- endif -%}
