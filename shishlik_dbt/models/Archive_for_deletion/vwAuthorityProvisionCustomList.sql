with
    field_value as (
        select
            apcv.AuthorityProvisionCustomValue_AuthorityProvisionId AuthorityProvision_Id,
            apcv.AuthorityProvisionCustomValue_FieldName + ' = ' + apcv.AuthorityProvisionCustomValue_Value FieldValue
        from {{ ref("vwAuthorityProvisionCustomValue") }} apcv
        where apcv.AuthorityProvisionCustomValue_AuthorityProvisionId is not null
    )
select AuthorityProvision_Id, count(*) num_FieldValue, '[' + string_agg(FieldValue, '],<br>[') + ']' FieldValueList
from field_value
group by AuthorityProvision_Id
