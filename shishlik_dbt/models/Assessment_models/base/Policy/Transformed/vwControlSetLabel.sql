with
    base as (select * from {{ ref("vwAbpSettings") }}),
    final_row as (
        select distinct
            AbpSettings_TenantId Tenant_Id,
            case
                AbpSettings_Name
                when 'Custom.Control.Label.Singular'
                then AbpSettings_Value
            end LabelSingular,
            case
                AbpSettings_Name
                when 'Custom.Control.Label.Plural'
                then AbpSettings_Value
            end LabelPlural
        from base
        where
            AbpSettings_Name
            in ('Custom.Control.Label.Plural', 'Custom.Control.Label.Singular')
    ),
    final as (
        select
            Tenant_Id,
            string_agg(LabelSingular, ', ') LabelSingular,
            string_agg(LabelPlural, ', ') LabelPlural
        from final_row
        group by Tenant_Id
    )
select *
from final
