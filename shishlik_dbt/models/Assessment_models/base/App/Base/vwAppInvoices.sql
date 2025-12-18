{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [InvoiceDate],
            cast([InvoiceNo] as nvarchar(4000)) InvoiceNo,
            cast([TenantAddress] as nvarchar(4000)) TenantAddress,
            cast([TenantLegalName] as nvarchar(4000)) TenantLegalName,
            cast([TenantTaxNo] as nvarchar(4000)) TenantTaxNo
        from {{ source("assessment_models", "AppInvoices") }}
    )

select
    {{ col_rename("Id", "AppInvoices") }},
    {{ col_rename("InvoiceDate", "AppInvoices") }},
    {{ col_rename("InvoiceNo", "AppInvoices") }},
    {{ col_rename("TenantAddress", "AppInvoices") }},

    {{ col_rename("TenantLegalName", "AppInvoices") }},
    {{ col_rename("TenantTaxNo", "AppInvoices") }}
from base
