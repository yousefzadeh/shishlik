{# 
DOC START
  - name: DB_log_table
    config:
      materialized: incremental
      contract:
        enforced: true
    columns:
      - name: ID
        data_type: int
        constraints:
          - type: not_null
      - name: SQL_name
        data_type: varchar(max)    
      - name: SQL_string
        data_type: varchar(max)    
      - name: StartTime
        data_type: datetime    
      - name: EndTime
        data_type: datetime    
DOC END
#}
{{-
    config(
        {
            "materialized": "incremental",
            "as_columnstore": false,
            "unique_key": "ID",
            "pre-hook": "{{ incremental_drop_all_indexes_on_table() }}",
            "post-hook": [
                "{{ truncate_relation(this)}}",
                "alter table {{ this }} add ID INT IDENTITY CONSTRAINT PK_DB_log_table PRIMARY KEY",
                "alter table {{ this }} alter column SQL_name VARCHAR(200) NOT NULL",
                "alter table {{ this }} alter column SQL_string VARCHAR(MAX) NOT NULL",
                "alter table {{ this }} alter column StartTime DATETIME NOT NULL",
                "alter table {{ this }} alter column EndTime DATETIME NULL"
            ]
        }
    )
-}}
with dummy_data as (
    select
        'SQL_name' as SQL_name,
        'SQL_string' as SQL_string,
        getdate() as StartTime,
        NULL as EndTime
)
select * from dummy_data