{# 
DOC START
  - name: DB_Views
    description: |
        List of all tables in the database from the list of objects
        List of Object Types:
        FN	SQL_SCALAR_FUNCTION
        SQ	SERVICE_QUEUE
        F 	FOREIGN_KEY_CONSTRAINT
        U 	USER_TABLE
        D 	DEFAULT_CONSTRAINT
        PK	PRIMARY_KEY_CONSTRAINT
        V 	VIEW
        IT	INTERNAL_TABLE
        P 	SQL_STORED_PROCEDURE
        TR	SQL_TRIGGER
        List of Tables is filter on "U"

    columns:
      - name: name
        description: Name of View
      - name: object_id
      - name: principal_id
      - name: schema_id
      - name: parent_object_id
      - name: type
        description: Object Type "V" is User View
      - name: type_desc
        description: Type Description
      - name: create_date
      - name: modify_date
      - name: is_ms_shipped
      - name: is_published
      - name: is_schema_published
DOC END
#}

with obj as (
    SELECT
    [name],
    object_id,
    principal_id,
    schema_id,
    parent_object_id,
    [type],
    [type_desc],
    create_date,
    modify_date,
    is_ms_shipped,
    is_published,
    is_schema_published
    FROM sys.objects
)
select * from obj
where [type] = 'V'