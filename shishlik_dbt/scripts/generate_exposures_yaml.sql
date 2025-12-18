-- Generates single exposures yaml file for all views in spinup production folder
-- Run against SPINUP YF Config DB
with
    user_list as (
        select per.ipperson, per.fullname, ic.emailaddress
        from person per
        left join
            ipcontact ic
            on per.ipperson = ic.ipid
            and ic.contacttypecode = 'EMAIL'
            and ic.contactcode = 'WORK'
    ),
    view_list as (
        select
            rv.viewid,
            rv.viewname,
            rv.viewdescription,
            rv.businessdescription,
            rv.technicaldescription,
            rv.creationdate,
            rv.ipcreator,
            created_by.fullname creator_fullname,
            created_by.emailaddress creator_email,
            rv.lastmodifieddate,
            rv.lastmodifiedtime,
            rv.iplastmodifier,
            last_modified_by.fullname modifier_fullname,
            last_modified_by.emailaddress modifier_email,
            rv.publishtimestamp,
            rv.contentcategorycode,
            rv.contentsubcategorycode,
            rv.ipapprover,
            rv.iptester
        from dbo.reportview as rv
        inner join
            dbo.orgreferencecodedesc as org on rv.contentcategorycode = org.refcode
        inner join user_list as created_by on rv.ipcreator = created_by.ipperson
        inner join
            user_list as last_modified_by
            on rv.iplastmodifier = last_modified_by.ipperson
        where
            (
                (org.shortdescription in ('6clicks Reports & Dashboards'))
                and rv.viewstatuscode in ('OPEN')
                and rv.viewtypecode in ('DRAGANDDROP', 'FREEHANDSQL')
            )
    ),
    report_view as (
        select *
        from reportview vh
        where
            -- show active views
            vh.viewstatuscode = 'OPEN'
            -- show views
            and vh.viewtypecode = 'DRAGANDDROP'
    ),
    report_view_tables as (
        select vh.viewid, vh.viewname, vt.viewname viewtablename
        from reportview vh
        left join reportview vt on vh.viewid = vt.parentviewid
        where
            -- show active views and tables
            vh.viewstatuscode = 'OPEN'
            and vt.viewstatuscode = 'OPEN'
            -- show views
            and vh.viewtypecode = 'DRAGANDDROP'
            -- show table objects
            and vt.viewtypecode = 'CHILDELEMENT'
    ),
    -- Virtual table
    report_view_virtual_tables as (
        select 
            vh.viewid viewid,
            vh.viewdescription,
            vh.viewname,
            vh.contentcategorycode,
            vh.contentsubcategorycode,
            vt.viewid viewtableid,
            coalesce(vt.viewdescription, vt.viewname) viewtablename,
            vt.viewstatuscode,
            rvs.sourcename,
            vt.viewtypecode,
            convert(
                varchar(max),
                cast(
                    '' as xml
                ).value('xs:base64Binary(sql:column("DataChunk"))', 'VARBINARY(MAX)')
            ) as sql_text
        from reportview vh
        inner join reportview vt on vh.viewid = vt.parentviewid
        inner join reportviewsource rvs on vt.sourceid = rvs.sourceid
        left join documentitem di on vt.viewid = di.subjectid
        left join documentrevision dr on di.documentid = dr.documentid
        left join documentdata dd on dr.revisionid = dd.revisionid
        where
            vh.viewstatuscode = 'OPEN'
            and vh.viewtypecode = 'DRAGANDDROP'
            -- and vt.ViewStatusCode = 'OPEN' 
            and vt.viewtypecode in ('FREEHANDSQL', 'VIRTUAL')
            and dr.revisionstatuscode = 'OPEN'
            and dr.publishflag = 1
            and vh.contentcategorycode = '6CLICKSREPORTSDASHBOARDS'
    ),
    virtual_table_split_dot as (
        select
            viewid,
            viewtableid,
            viewtablename,
            replace(
                replace(
                    replace(replace(s.value, char(194), ' '), char(160), ' '),
                    char(13),
                    ' '
                ),
                char(10),
                ' '
            ) sql_fragment
        from
            report_view_virtual_tables
            cross apply string_split(replace(sql_text, '"', ''), '.') s
    ),
    virtual_table_sql_frag_order as (
        select
            viewid,
            viewtableid,
            viewtablename,
            sql_fragment,
            row_number() over (
                partition by viewid, viewtableid order by (select null)
            ) row_no,
            lead(sql_fragment) over (
                partition by viewid, viewtableid order by (select null)
            ) next_sql
        from virtual_table_split_dot
    ),
    virtual_table_ref as (
        select distinct
            viewid,
            viewtableid,
            '"'
            + substring(next_sql, 1, patindex('% %', next_sql) - 1)
            + '"' db_table_ref
        from virtual_table_sql_frag_order
        where sql_fragment like '%reporting' and patindex('% %', next_sql) > 0
    ),
    -- Custom sql in filters
    yf_custom_filter as (
        select distinct
            o.orgname as vieworgname,
            rvs.sourcename as viewsourcename,
            rv.contentcategorycode,
            rv.contentsubcategorycode,
            case
                when rv.viewname is null
                then 'Report Cached Filter'
                when rv.viewname is not null
                then 'View Cached Filter'
            end as isreportorviewcachedfilter,
            rv.viewid,
            rv.viewdescription as viewname,
            rv.viewstatuscode,
            rft1.fieldtemplateid as filtergroupid,
            rft1.shortdescription as filtergroupname,
            cf. [filterid],
            rft2.shortdescription as viewfieldname,
            rft2.datatypecode,
            rv1.viewname as tablename,
            rft2.columnname,
            -- ,cf.[CachedFilterTypeCode]
            -- cf. [cachedfilterid],
            case
                when cf. [cachedfiltertypecode] = 'CACHEDONDEMAND'
                then 'Cached on Demand'
                when cf. [cachedfiltertypecode] = 'CACHEDFILTER'
                then 'Cached'
                when cf. [cachedfiltertypecode] = 'QUERYONDEMAND'
                then 'Custom Query on Demand'
                when cf. [cachedfiltertypecode] = 'CUSTOMQUERYFILTER'
                then 'Custom Query'
                when cf. [cachedfiltertypecode] = 'SOURCEFILTER'
                then 'Source Filter'
                else cf. [cachedfiltertypecode]
            end as cachedfiltertype,
            cf. [cachetime],
            td.datachunk sql_text
        from [dbo]. [cachedfilter] cf
        inner join
            [dbo]. [reportfieldtemplate] rft1 on cf.reportid = rft1.fieldtemplateid
        left join [dbo]. [reportfilter] rf on cf.filterid = rf.filterid
        left join
            [dbo]. [reportfieldtemplate] rft2
            on rf.fieldtemplateid = rft2.fieldtemplateid
        left join [dbo]. [reportview] rv on rft1.viewid = rv.viewid
        left join [dbo]. [reportview] rv1 on rft2.subviewid = rv1.viewid
        left join [dbo]. [reportviewsource] rvs on rv.sourceid = rvs.sourceid
        left join [dbo]. [organisation] o on rvs.iporg = o.iporg
        left join [dbo]. [textdata] td on cf.textid = td.textid
        where
            rv.viewstatuscode = 'OPEN'
            and rv.viewname is not null  -- only view-level cached filters
            and rft1.statuscode = 'OPEN'
            and rft2.statuscode = 'OPEN'
    -- and cf.CachedFilterTypeCode = 'CUSTOMQUERYFILTER'
    ),
    split_dot as (
        select
            viewid,
            filtergroupid,
            filtergroupname,
            viewfieldname,
            filterid,
            replace(
                replace(
                    replace(replace(s.value, char(194), ' '), char(160), ' '),
                    char(13),
                    ' '
                ),
                char(10),
                ' '
            ) sql_fragment
        from
            yf_custom_filter cross apply string_split(replace(sql_text, '"', ''), '.') s
    ),
    sql_frag_order as (
        select
            viewid,
            filtergroupid,
            filtergroupname,
            viewfieldname,
            filterid,
            sql_fragment,
            row_number() over (
                partition by viewid, filtergroupid order by (select null)
            ) row_no,
            lead(sql_fragment) over (
                partition by viewid, filtergroupid order by (select null)
            ) next_sql
        from split_dot
    ),
    custom_filter_ref as (
        select distinct
            viewid,
            filterid,
            next_sql,
            patindex('% %', next_sql) i1,
            patindex('%[ ]%', next_sql) i2,
            '"'
            + substring(next_sql, 1, patindex('% %', next_sql) - 1)
            + '"' db_table_ref
        from sql_frag_order
        where sql_fragment like '%reporting' and patindex('% %', next_sql) > 0
    ),
    -- Form the exposure text
    -- Header lines 
    exposure_description as (
        select
            vh.viewid,
            -1 view_line_no,
            '      description: |'
            + char(13)
            + char(10)
            + '        '
            + '### **View Name:** '
            + coalesce(vh.viewdescription, vh.viewname)
            + char(13)
            + char(10)
            + char(13)
            + char(10)
            + '        '
            + '**Description:** '
            + replace(
                coalesce(vh.businessdescription, 'No Description'),
                char(13) + char(10),
                char(13) + char(10) + '        '
            )
            + char(13)
            + char(10)
            + char(13)
            + char(10)
            + '        '
            + '**View ID:** '
            + cast(vh.viewid as varchar)
            + char(13)
            + char(10)
            + char(13)
            + char(10)
            + '        '
            + '**Folder:** '
            + vh.contentcategorycode
            + ' / '
            + vh.contentsubcategorycode
            + char(13)
            + char(10)
            + char(13)
            + char(10)
            + '        '
            + '**Created By:** '
            + vl.creator_fullname
            + ' / '
            + vl.creator_email
            + ' on '
            + cast(vl.creationdate as varchar)
            + char(13)
            + char(10)
            + char(13)
            + char(10)
            + '        '
            + '**Modified By:** '
            + vl.modifier_fullname
            + ' / '
            + vl.modifier_email
            + ' on '
            + cast(vl.lastmodifieddate as varchar)
            + char(13)
            + char(10)
            + char(13)
            + char(10) as t
        from report_view vh
        join view_list vl on vh.viewid = vl.viewid
    ),
    exposure_header as (
        select
            0 viewid,
            0 view_line_no,
            '# Generated on ' + cast(getdate() as varchar) + ' (UTC)' as t
        union all
        select 0 viewid, 1 view_line_no, 'version: 2' as t
        union all
        select 0 viewid, 2 view_line_no, '' as t
        union all
        select 0 viewid, 3 view_line_no, 'exposures:' as t
    ),
    -- Body lines - all views
    exposure_virtual_table_desc as (
        select distinct
            vh.viewid,
            vt.viewtableid view_line_no,
            '        '
            + '**Virtual Table Name:** '
            + vt.viewtablename
            + ' [#'
            + cast(vt.viewtableid as varchar)
            + ']'
            + char(13)
            + char(10)
            + char(13)
            + char(10)
            -- '        ' + '**Data Source:** '+ vt.SourceName + CHAR(13) + CHAR(10) +
            -- CHAR(13) + CHAR(10) +
            -- '        ' + '**Virtual Table ID:** ' + cast(vt.ViewTableId as varchar)
            -- + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) +
            + '        '
            + '**SQL:** '
            + char(13)
            + char(10)
            + char(13)
            + char(10)
            + '        '
            + '    '
            + replace(vt.sql_text, char(10), char(10) + '        ' + '    ')
            + char(13)
            + char(10)
            + char(13)
            + char(10)
            + '        '
            + char(13)
            + char(10)
            + char(13)
            + char(10) as t
        from report_view_virtual_tables vt
        join view_list vh on vh.viewid = vt.viewid
        where vt.sql_text is not null
    ),
    exposure_custom_filter_desc as (
        select distinct
            viewid,
            filterid view_line_no,
            -- '        ' + '**Filter Group Name:** '+ FilterGroupName + CHAR(13) +
            -- CHAR(10) + CHAR(13) + CHAR(10) +
            -- '        ' + '**Data Source:** '+ ViewSourceName + CHAR(13) + CHAR(10)
            -- + CHAR(13) + CHAR(10) +
            -- '        ' + '**Filter Field Name:** '+ ViewFieldName + CHAR(13) +
            -- CHAR(10) + CHAR(13) + CHAR(10) +
            '        '
            + '**Filter Group / Name:** '
            + filtergroupname
            + ' / '
            + viewfieldname
            + ' [#'
            + cast(filterid as varchar)
            + ']'
            + char(13)
            + char(10)
            + char(13)
            + char(10)
            -- '        ' + '**Filter ID:** ' + cast(FilterId as varchar) + CHAR(13) +
            -- CHAR(10) + CHAR(13) + CHAR(10) +
            + '        '
            + '**SQL:** '
            + char(13)
            + char(10)
            + char(13)
            + char(10)
            -- '        ' + '    ' + replace(replace(string_agg(SQL_text,''),
            -- char(10), ''), CHAR(13), CHAR(13) + CHAR(10) + '        ' + '    ') +
            -- CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) +
            -- '        ' + '    ' + string_agg(SQL_text,'') +
            + '        '
            + '    '
            + replace(
                string_agg(sql_text, ''), char(10), char(10) + '        ' + '    '
            )
            + char(13)
            + char(10)
            + char(13)
            + char(10)
            + '        '
            + char(13)
            + char(10)
            + char(13)
            + char(10) as t
        from yf_custom_filter
        where sql_text is not null
        group by viewid, viewsourcename, filtergroupname, viewfieldname, filterid
    ),
    exposure_body as (
        -- Header lines for each view
        select
            vh.viewid,
            -5 view_line_no,
            '    - name: ' + replace(
                replace(replace(vh.viewdescription, ' ', '_'), ')', ''), '(', ''
            ) as t
        from report_view vh
        union all
        select vh.viewid, -4.5 view_line_no, '      label: ' + vh.viewdescription as t
        from report_view vh
        union all
        select vh.viewid, -4 view_line_no, '      type: dashboard' as t
        from report_view vh
        union all
        select vh.viewid, -3 view_line_no, '      maturity: medium' as t
        from report_view vh
        union all
        select
            vh.viewid,
            -2 view_line_no,
            '      url: https://yellowfin-dev-ihsopk.6clicks.io/logoff.i4' as t
        from report_view vh
        union all
        select viewid, -1 view_line_no, t
        from exposure_description
        union all
        select
            viewid,
            -0.95 view_line_no,
            '        ' + '### **Virtual Tables Embedded SQL** ' t
        from report_view vh
        union all
        select viewid, -0.9 view_line_no, t
        from exposure_virtual_table_desc
        union all
        select
            viewid,
            -0.85 view_line_no,
            '        ' + '### **Custom Filter Embedded SQL** ' t
        from report_view vh
        union all
        select viewid, -0.8 view_line_no, t
        from exposure_custom_filter_desc
        union all
        select vh.viewid, 0 view_line_no, '      depends_on:' as t
        from reportview vh
    ),
    exposure_ref_tables as (
        -- DB Tables and DB Views referenced by the view
        select
            rvt.viewid,
            rvt.viewid view_line_no,
            '          - ref('
            + replace(rvt.viewtablename, '"reporting".', '')
            + ')' as t
        from report_view_tables rvt
    ),
    exposure_virtual_tables as (
        -- DB Views referenced by Virtual Tables
        select distinct
            vh.viewid,
            vt.viewtableid view_line_no,
            '          - ref(' + vt.db_table_ref + ')' as t
        from report_view vh
        join virtual_table_ref vt on vh.viewid = vt.viewid
    ),
    exposure_custom_filters as (
        -- DB Views referenced by Custom filter SQL
        select distinct
            vh.viewid,
            cf.filterid view_line_no,
            '          - ref(' + cf.db_table_ref + ')' as t
        from report_view vh
        join custom_filter_ref cf on vh.viewid = cf.viewid
    ),
    exposure_content as (
        select distinct *
        from
            (
                select viewid, 1 as view_line_no, t
                from exposure_ref_tables
                union all
                select viewid, 1 as view_line_no, t
                from exposure_virtual_tables
                union all
                select viewid, 1 as view_line_no, t
                from exposure_custom_filters
            ) as t
    ),
    exposure_footer as (
        -- Footer lines for each view
        select vh.viewid, 1001 view_line_no, '      owner:' as t
        from report_view vh
        union all
        select
            vh.viewid, 1002 view_line_no, '          name: ' + vl.creator_fullname as t
        from report_view vh
        join view_list vl on vh.viewid = vl.viewid
        union all
        select vh.viewid, 1003 view_line_no, '          email: ' + vl.creator_email as t
        from report_view vh
        join view_list vl on vh.viewid = vl.viewid
        union all
        select vh.viewid, 10000000 view_line_no, ''
        from report_view vh
        join view_list vl on vh.viewid = vl.viewid
    ),
    exposure_virtual_table_comments as (
        -- select distinct vh.viewid, -0.7 view_line_no,	'        ' + '* Virtual
        -- table: ' + cast(vt.ViewTableId as varchar) + CHAR(13) + CHAR(10) as t from
        -- report_view_virtual_tables vt join view_list vh on vh.ViewId = vt.ViewId
        -- union all
        select distinct
            vh.viewid,
            vt.viewtableid view_line_no,
            '# Data Source: '
            + vt.sourcename
            + char(13)
            + char(10)
            + '# Virtual Table Name: '
            + vt.viewtablename
            + char(13)
            + char(10)
            + '# Virtual Table ID: '
            + cast(vt.viewtableid as varchar)
            + char(13)
            + char(10)
            + '# SQL: '
            + char(13)
            + char(10)
            + '#      '
            + replace(vt.sql_text, char(13) + char(10), char(13) + char(10) + '#      ')
            + char(13)
            + char(10)
            + '    '
            + char(13)
            + char(10) as t
        from report_view_virtual_tables vt
        join view_list vh on vh.viewid = vt.viewid
    ),
    exposure_custom_filter_comments as (
        select distinct
            viewid,
            filterid view_line_no,
            '# Data Source: '
            + viewsourcename
            + char(13)
            + char(10)
            + '# Filter Group Name: '
            + filtergroupname
            + char(13)
            + char(10)
            + '# Filter Field Name: '
            + viewfieldname
            + char(13)
            + char(10)
            + '# Filter ID: '
            + cast(filterid as varchar)
            + char(13)
            + char(10)
            + '# SQL: '
            + char(13)
            + char(10)
            + '#      '
            + replace(
                replace(string_agg(sql_text, ''), char(10), ''),
                char(13),
                char(13) + char(10) + '#      '
            )
            + char(13)
            + char(10)
            + '    '
            + char(13)
            + char(10) as t
        from yf_custom_filter
        group by viewid, viewsourcename, filtergroupname, viewfieldname, filterid
    ),
    exposure_comments as (
        select *
        from exposure_virtual_table_comments
        union all
        select *
        from exposure_custom_filter_comments
    ),
    exposure_text as (
        select *
        from exposure_header
        union all
        select *
        from exposure_body
        union all
        select *
        from exposure_content
        union all
        select *
        from exposure_footer
    -- union all
    -- select * from exposure_comments
    ),
    final as (
        select viewid, view_line_no, t as text_line
        from exposure_text et
        where
            et.viewid in (
                select vl.viewid
                from view_list vl
                -- list only views with tables from database (will not list Views
                -- using only virtual tables)
                join report_view_tables rvt on vl.viewid = rvt.viewid
                -- Add zero to include header rows
                union all
                select 0
            )
    )
select text_line
from final
order by viewid, view_line_no
