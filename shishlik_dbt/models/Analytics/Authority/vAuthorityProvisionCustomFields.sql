with cus_field as(
select
ap.AuthorityId Authority_Id,
ap.Id AuthorityProvision_Id,
js.AuthorityProvision_CustomFieldId,
js.AuthorityProvision_CustomField,
js.AuthorityProvision_CustomFieldValue
from {{ source("authority_ref_models", "AuthorityProvision") }} ap
outer apply openjson(ap.CustomDataJson) WITH (  
                    AuthorityProvision_CustomFieldId Int             '$.Id',
                    AuthorityProvision_CustomField Varchar(200)  '$.Name',
                    AuthorityProvision_CustomFieldValue    nvarchar(max)    '$.Value'
                    ) as js
where ap.IsDeleted = 0
)

select
ap.Authority_Id,
ap.AuthorityProvision_Id,
apcf.FieldType AuthorityProvision_CustomFieldTypeCode,
case 
when apcf.FieldType = 1 then 'Singleline'
when apcf.FieldType = 0 then 'Multiline'
end AuthorityProvision_CustomFieldType,
apcf.[Order] AuthorityProvision_CustomFieldOrder,
ap.AuthorityProvision_CustomFieldId,
ap.AuthorityProvision_CustomField,
ap.AuthorityProvision_CustomFieldValue
from cus_field ap
join {{ source("authority_ref_models", "AuthorityProvisionCustomField") }} apcf
on apcf.AuthorityId = ap.Authority_Id
and apcf.Id = ap.AuthorityProvision_CustomFieldId and apcf.IsDeleted = 0