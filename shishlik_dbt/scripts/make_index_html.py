#!/usr/bin/env python
# coding: utf-8

import json
import re
import os

PATH_DBT_PROJECT = os.getcwd()

# Original index.html content
with open(os.path.join(PATH_DBT_PROJECT, 'target', 'index.html'), 'r') as f:
    content_index = f.read()

# Load manifest and catalog json files    
with open(os.path.join(PATH_DBT_PROJECT, 'target', 'manifest.json'), 'r') as f:
    json_manifest = json.loads(f.read())
    # Drop elements we do not want to include
    IGNORE_PROJECTS = ['codegen', 'dbt_utils', 'dbt_date','tsql_utils' ]
    for element_type in ['nodes', 'sources', 'macros', 'parent_map', 'child_map']:  # navigate into manifest
        # We transform to list to not change dict size during iteration, we use default value {} to handle KeyError
        for key in list(json_manifest.get(element_type, {}).keys()):  
            for ignore_project in IGNORE_PROJECTS:
                if re.match(fr'^.*\.{ignore_project}\.', key):  # match with string that start with '*.<ignore_project>.'
                    del json_manifest[element_type][key]  # delete element
    manifest_json = json.dumps(json_manifest)

with open(os.path.join(PATH_DBT_PROJECT, 'target', 'catalog.json'), 'r') as f:
    json_catalog = json.loads(f.read())
    catalog_json = json.dumps(json_catalog)


# create single index.html file in public folder
with open(os.path.join(PATH_DBT_PROJECT, 'public', 'index.html'), 'w') as f:
    # Replace the reference to the original manifest and catalog json files with the actual json content
    search_str =           'o=[i("manifest","manifest.json"+t),i("catalog","catalog.json"+t)]'
    #                                        ^^^^^^^^^^^^^                  ^^^^^^^^^^^^ 
    #                                        replace with V                  replace with  V
    new_str = "o=[{label: 'manifest', data: "+manifest_json+"},{label: 'catalog', data: "+catalog_json+"}]"
    new_content = content_index.replace(search_str, new_str)
    f.write(new_content)