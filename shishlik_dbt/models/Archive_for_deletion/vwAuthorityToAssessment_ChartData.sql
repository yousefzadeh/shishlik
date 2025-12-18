with
    base as (
        select
            a.Template_TenantId Tenant_Id,
            a.Authority_Id,
            a.Template_Id,
            a.Template_Name,
            a.Template_Version,
            a.AuthorityProvision_Id,
            a.Answer_Score,
            c.AuthorityProvisionCustomField_FieldNameValue GroupName,
            a.Answer_QuestionId,
            a.Assessment_Id
        from {{ ref("vwAuthorityToAssessmentDetail") }} a
        join
            {{ ref("vwAuthorityProvisionCustomTable") }} c
            on a.Authority_Id = c.Authority_Id
            and a.AuthorityProvision_Id = c.AuthorityProvision_Id
        where c.AuthorityProvisionCustomField_Order = 1 and a.Answer_Score is not NULL
    ),
    agg_prov as (
        -- risk rating at provision level
        select
            Tenant_Id,
            Authority_Id,
            Template_Id,
            Template_Name,
            Template_Version,
            AuthorityProvision_Id,
            GroupName,
            cast(round(avg(Answer_Score), 0) as varchar(10)) avg_score,
            cast(round(min(Answer_Score), 0) as varchar(10)) min_score,
            cast(round(max(Answer_Score), 0) as varchar(10)) max_score,
            count(distinct Answer_QuestionId) num_questions,
            count(distinct Assessment_Id) num_assessments,
            1 num_provisions
        from base
        group by Tenant_Id, Authority_Id, Template_Id, Template_Name, Template_Version, AuthorityProvision_Id, GroupName
    ),
    -- Create 3 separate tables to union into a long table broken by roll up method
    -- Roll up at Provision level
    agg_prov_avg as (
        -- provision count per risk rating at Provision level
        select
            Tenant_Id,
            Authority_Id,
            Template_Id,
            Template_Name,
            Template_Version,
            AuthorityProvision_Id,
            GroupName,
            'Average' RollUpMethod,
            avg_score risk_score,
            1 num_provisions
        from agg_prov
    ),
    agg_prov_min as (
        -- provision count per risk rating at Provision level
        select
            Tenant_Id,
            Authority_Id,
            Template_Id,
            Template_Name,
            Template_Version,
            AuthorityProvision_Id,
            GroupName,
            'Minimum' RollUpMethod,
            min_score risk_score,
            1 num_provisions
        from agg_prov
    ),
    agg_prov_max as (
        -- provision count per risk rating at Provision level
        select
            Tenant_Id,
            Authority_Id,
            Template_Id,
            Template_Name,
            Template_Version,
            AuthorityProvision_Id,
            GroupName,
            'Maximum' RollUpMethod,
            max_score risk_score,
            1 num_provisions
        from agg_prov
    ),
    -- Aggregate roll up to Template level
    agg_template_avg as (
        -- provision count per risk rating at template level
        select
            Tenant_Id,
            Authority_Id,
            Template_Id,
            Template_Name,
            Template_Version,
            GroupName,
            'Average' RollUpMethod,
            avg_score risk_score,
            count(distinct AuthorityProvision_Id) num_provisions
        from agg_prov
        group by Tenant_Id, Authority_Id, Template_Id, Template_Name, Template_Version, GroupName, avg_score
    ),
    agg_template_min as (
        -- provision count per risk rating at template level
        select
            Tenant_Id,
            Authority_Id,
            Template_Id,
            Template_Name,
            Template_Version,
            GroupName,
            'Minimum' RollUpMethod,
            min_score risk_score,
            count(distinct AuthorityProvision_Id) num_provisions
        from agg_prov
        group by Tenant_Id, Authority_Id, Template_Id, Template_Name, Template_Version, GroupName, min_score
    ),
    agg_template_max as (
        -- provision count per risk rating at template level
        select
            Tenant_Id,
            Authority_Id,
            Template_Id,
            Template_Name,
            Template_Version,
            GroupName,
            'Maximum' RollUpMethod,
            max_score risk_score,
            count(distinct AuthorityProvision_Id) num_provisions
        from agg_prov
        group by Tenant_Id, Authority_Id, Template_Id, Template_Name, Template_Version, GroupName, max_score
    ),
    -- Long table for YF Chart
    agg_prov_union as (
        -- Long table at provision level
        select *
        from agg_prov_avg

        union all

        select *
        from agg_prov_min

        union all

        select *
        from agg_prov_max

    ),
    agg_template_union as (
        -- Long table at Template level
        select *
        from agg_template_avg

        union all

        select *
        from agg_template_min

        union all

        select *
        from agg_template_max

    )

select
    Tenant_Id,
    atu.Authority_Id,
    a.Authority_Name,
    Template_Id,
    Template_Name,
    Template_Version,
    -- AuthorityProvision_Id, -- remove this when using template level agg
    GroupName,
    RollUpMethod,
    case
        when risk_score = '0'
        then 'No Risk'
        when risk_score = '1'
        then 'Very Low'
        when risk_score = '2'
        then 'Low'
        when risk_score = '3'
        then 'Medium'
        when risk_score = '4'
        then 'High'
        when risk_score = '5'
        then 'Very High'
        else 'Undefined'
    end as Risk_Scale,
    num_provisions
from agg_template_union atu
join {{ ref("vwAuthority") }} a on atu.Authority_Id = a.Authority_Id
