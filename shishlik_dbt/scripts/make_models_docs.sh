#! /bin/bash

cat scripts/models_docs_header.yml > models/models_docs.yml
find models -name "*.sql" -exec scripts/extract_model_docs.awk {} \; >> models/models_docs.yml