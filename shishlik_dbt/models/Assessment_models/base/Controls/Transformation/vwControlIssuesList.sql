with
    control_issue as (
        select distinct
            ics.IssueControlStatement_TenantId Tenant_Id,
            ics.IssueControlStatement_ControlId Control_Id,
            i.Issues_Name
        from {{ ref("vwIssueControlStatement") }} ics
        inner join
            {{ ref("vwIssues") }} i
            on i.Issues_Id = ics.IssueControlStatement_IssueId
            and i.Issues_TenantId = ics.IssueControlStatement_TenantId
    ),
    final as (
        select
            Tenant_Id,
            Control_Id,
            string_agg(cast(Issues_Name as varchar(max)), ', ') Issues_List
        from control_issue
        group by Tenant_Id, Control_Id
    )
select *
from final
