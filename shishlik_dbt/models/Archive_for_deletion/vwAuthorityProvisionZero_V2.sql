with
    prov as (
        select
            AuthorityProvision_Id,
            AuthorityProvision_Name,
            AuthorityProvision_ReferenceId,
            AuthorityProvision_Description,
            AuthorityProvision_URL,
            AuthorityProvision_AuthorityId,
            AuthorityProvision_CustomDataJson,
            AuthorityProvision_Order,
            AuthorityProvision_UpdateTime
        from {{ ref("vwAuthorityProvision") }}
    ),
    zero as (
        select
            0 AuthorityProvision_Id,
            'Unassigned' AuthorityProvision_Name,
            '0' AuthorityProvision_ReferenceId,
            'Unassigned' AuthorityProvision_Description,
            '' AuthorityProvision_URL,
            Authority_Id AuthorityProvision_AuthorityId,
            '[{"Id": 1,"Name": "Unassigned","Value": "Blank","FieldType": null,"FieldTypeId": 0}]' AuthorityProvision_CustomDataJson
            ,
            1 AuthorityProvision_Order,
            cast('2000-01-01 00:00:01.000' as DateTime) AuthorityProvision_UpdateTime
        from {{ ref("vwAuthority") }}
    ),
    final as (
        select *
        from prov
        union all
        select *
        from zero
    )
select *
from final
