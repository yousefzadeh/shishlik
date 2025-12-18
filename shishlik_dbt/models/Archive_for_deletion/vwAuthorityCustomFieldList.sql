-- Grain: One row per Authority 
-- Concatenated list of Custom Field names
with
    auth_field as (
        select distinct
            acf.AuthorityProvisionCustomField_AuthorityId AuthorityCustomField_AuthorityId,
            acf.AuthorityProvisionCustomField_FieldName AuthorityCustomField_FieldName
        from {{ ref("vwAuthorityProvisionCustomField") }} acf
        where acf.AuthorityProvisionCustomField_IsDeleted = 0
    )
select
    acf.AuthorityCustomField_AuthorityId,
    '[ "' + string_agg(acf.AuthorityCustomField_FieldName, '","') + '" ]' CustomField
from auth_field acf
group by acf.AuthorityCustomField_AuthorityId
