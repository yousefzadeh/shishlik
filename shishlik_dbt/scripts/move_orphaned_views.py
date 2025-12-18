#!/usr/bin/env python
# coding: utf-8

# # Move unreferenced models to archived folder

# In[ ]:


import json 
import pandas as pd
import os 
import numpy as np


# In[ ]:


os.getcwd()


# In[ ]:


os.chdir("target/")


# ## Build object dataframe from manifest.json
# 
# ### 1. Load manifest.json
# 
# Top level keys
# 
# * metadata
# * nodes
# * sources
# * macros
# * docs
# * exposures
# * metrics
# * selectors
# * disabled
# * parent_map
# * child_map

# In[ ]:


with open('./manifest.json', 'r') as f:
    data = json.load(f)


# ### 2. Union all the objects

# ### 1. nodes
# Example json fragment of one entry
# ```
# "model.sixclicks_dev_orig.vwWorkflowStage": {
# 			"compiled": true,
# 			"resource_type": "model",
# 			"depends_on": {
# 				"macros": [
# 					"macro.sixclicks_dev_orig.system_remove_IsDeleted",
# 					"macro.sixclicks_dev_orig.col_rename"
# 				],
# 				"nodes": [
# 					"source.sixclicks_dev_orig.workflow_models.WorkflowStage"
# 				]
# 			},
# 			"config": {
# 				"enabled": true,
# 				"alias": null,
# 				"schema": null,
# 				"database": null,
# 				"tags": [],
# 				"meta": {},
# 				"materialized": "view",
# 				"incremental_strategy": null,
# 				"persist_docs": {},
# 				"quoting": {},
# 				"column_types": {},
# 				"full_refresh": null,
# 				"unique_key": null,
# 				"on_schema_change": "ignore",
# 				"grants": {},
# 				"packages": [],
# 				"docs": {
# 					"show": true,
# 					"node_color": null
# 				},
# 				"post-hook": [],
# 				"pre-hook": []
# 			},
# 			"database": "6clicks-dev-ihsopk",
# 			"schema": "test_dbt_cicd",
# 			"fqn": [
# 				"sixclicks_dev_orig",
# 				"Workflow",
# 				"vwWorkflowStage"
# 			],
# 			"unique_id": "model.sixclicks_dev_orig.vwWorkflowStage",
# 			"raw_code": "select * from somewhere",
# 			"language": "sql",
# 			"package_name": "sixclicks_dev_orig",
# 			"root_path": "/home/herman/projects/6clicksReporting",
# 			"path": "Workflow/vwWorkflowStage.sql",
# 			"original_file_path": "models/Workflow/vwWorkflowStage.sql",
# 			"name": "vwWorkflowStage",
# 			"alias": "vwWorkflowStage",
# 			"checksum": {
# 				"name": "sha256",
# 				"checksum": "d44cd9fe65237ad53bb652e8fea8b0cde9091beca6b89ad9d8b89730173f52ff"
# 			},
# 			"tags": [],
# 			"refs": [],
# 			"sources": [
# 				[
# 					"workflow_models",
# 					"WorkflowStage"
# 				]
# 			],
# 			"metrics": [],
# 			"description": "",
# 			"columns": {},
# 			"meta": {},
# 			"docs": {
# 				"show": true,
# 				"node_color": null
# 			},
# 			"patch_path": null,
# 			"compiled_path": "target/compiled/sixclicks_dev_orig/models/Workflow/vwWorkflowStage.sql",
# 			"build_path": "target/run/sixclicks_dev_orig/models/Workflow/vwWorkflowStage.sql",
# 			"deferred": false,
# 			"unrendered_config": {},
# 			"created_at": 1682394408.451133,
# 			"compiled_code": "select * from somewhere",
# 			"extra_ctes_injected": true,
# 			"extra_ctes": [],
# 			"relation_name": "\"6clicks-dev-ihsopk\".\"test_dbt_cicd\".\"vwWorkflowStage\""
# 		}
# ```

# In[ ]:


nodes_obj = pd.DataFrame({ 
    "id": [ n for n in data["nodes"].keys() ],
    "depends_on" : [ n["depends_on"] for n in data["nodes"].values() ],
    "materialized" : [ n["config"]["materialized"] for n in data["nodes"].values() ],
    "original_file_path" : [ n["original_file_path"] for n in data["nodes"].values() ],
    "name" : [ n["name"] for n in data["nodes"].values() ] 
    })
# nodes_obj


# ### 2. sources
# Example fragment
# ```
# 		"source.sixclicks_dev_orig.workflow_models.Workflow": {
# 			"fqn": [
# 				"sixclicks_dev_orig",
# 				"Workflow",
# 				"workflow_models",
# 				"Workflow"
# 			],
# 			"database": "6clicks-dev-ihsopk",
# 			"schema": "dbo",
# 			"unique_id": "source.sixclicks_dev_orig.workflow_models.Workflow",
# 			"package_name": "sixclicks_dev_orig",
# 			"root_path": "/home/herman/projects/6clicksReporting",
# 			"path": "models/Workflow/workflow_models_reference_file.yml",
# 			"original_file_path": "models/Workflow/workflow_models_reference_file.yml",
# 			"name": "Workflow",
# 			"source_name": "workflow_models",
# 			"source_description": "",
# 			"loader": "",
# 			"identifier": "Workflow",
# 			"resource_type": "source",
# 			"quoting": {
# 				"database": null,
# 				"schema": null,
# 				"identifier": null,
# 				"column": null
# 			},
# 			"loaded_at_field": null,
# 			"freshness": {
# 				"warn_after": {
# 					"count": null,
# 					"period": null
# 				},
# 				"error_after": {
# 					"count": null,
# 					"period": null
# 				},
# 				"filter": null
# 			},
# 			"external": null,
# 			"description": "",
# 			"columns": {},
# 			"meta": {},
# 			"source_meta": {},
# 			"tags": [],
# 			"config": {
# 				"enabled": true
# 			},
# 			"patch_path": null,
# 			"unrendered_config": {},
# 			"relation_name": "\"6clicks-dev-ihsopk\".\"dbo\".\"Workflow\"",
# 			"created_at": 1682394414.8137027
# 		}
# ```

# In[ ]:


sources_obj = pd.DataFrame({ 
    "id": [ n for n in data["sources"].keys() ],
    "depends_on"   : [""] * len(data["sources"].keys()),
    "materialized" : ["table"] * len(data["sources"].keys()),
    "original_file_path" : [ n["original_file_path"] for n in data["sources"].values() ],
    "name" : [ n["name"] for n in data["sources"].values() ] 
    })
# sources_obj


# ### 3. macros
# Example json fragment
# ```
# 		"macro.sixclicks_dev_orig.test_incremental": {
# 			"unique_id": "macro.sixclicks_dev_orig.test_incremental",
# 			"package_name": "sixclicks_dev_orig",
# 			"root_path": "/home/herman/projects/6clicksReporting",
# 			"path": "macros/test_incremental_table.sql",
# 			"original_file_path": "macros/test_incremental_table.sql",
# 			"name": "test_incremental",
# 			"macro_sql": "{% macro blablabla() %}",
# 			"resource_type": "macro",
# 			"tags": [],
# 			"depends_on": {
# 				"macros": []
# 			},
# 			"description": "",
# 			"meta": {},
# 			"docs": {
# 				"show": true,
# 				"node_color": null
# 			},
# 			"patch_path": null,
# 			"arguments": [],
# 			"created_at": 1682394407.2099958,
# 			"supported_languages": null
# 		}
# ```

# In[ ]:


macros_obj = pd.DataFrame({ 
    "id": [ n for n in data["macros"].keys() ],
    "depends_on" : [ n["depends_on"] for n in data["macros"].values() ],
    "materialized" : ["macros"] * len(data["macros"].keys()),
    "original_file_path" : [ n["original_file_path"] for n in data["macros"].values() ],
    "name" : [ n["name"] for n in data["macros"].values() ] 
    })
# macros_obj


# ### 4. docs
# Example json fragment
# ```
# 		"dbt.__overview__": {
# 			"unique_id": "dbt.__overview__",
# 			"package_name": "dbt",
# 			"root_path": "/home/herman/.pyenv/versions/3.8/envs/dbt38/lib/python3.8/site-packages/dbt/include/global_project",
# 			"path": "overview.md",
# 			"original_file_path": "docs/overview.md",
# 			"name": "__overview__",
# 			"block_contents": "### Welcome!"
# 		}
# 	}
# ```

# In[ ]:


docs_obj = pd.DataFrame({ 
    "id": [ n for n in data["docs"].keys() ],
    "depends_on" : [""] * len(data["docs"].keys()),
    "materialized" : [""] * len(data["docs"].keys()),
    "original_file_path" : [ n["original_file_path"] for n in data["docs"].values() ],
    "name" : [ n["name"] for n in data["docs"].values() ] 
    })
# docs_obj


# ### 5. exposures
# Example json fragment
# ```
# 		"exposure.sixclicks_dev_orig.Attestations": {
# 			"fqn": [
# 				"sixclicks_dev_orig",
# 				"YellowfinReport_views",
# 				"exposures",
# 				"Attestations"
# 			],
# 			"unique_id": "exposure.sixclicks_dev_orig.Attestations",
# 			"package_name": "sixclicks_dev_orig",
# 			"root_path": "/home/herman/projects/6clicksReporting",
# 			"path": "YellowfinReport_views/exposures/spinup_production_202304181230.yml",
# 			"original_file_path": "models/YellowfinReport_views/exposures/spinup_production_202304181230.yml",
# 			"name": "Attestations",
# 			"type": "dashboard",
# 			"owner": {
# 				"email": "ban.pradhan@minerra.net",
# 				"name": "Ban Pradhan parent"
# 			},
# 			"resource_type": "exposure",
# 			"description": "* View Name: Attestations\n* Description: New View\n* View ID: 270882\n* Folder: 6CLICKSREPORTSDASHBOARDS / ATTESTATIONS\n* Created By: Ban Pradhan parent / ban.pradhan@minerra.net on 2022-02-21\n* Modified By: Ban Pradhan parent / ban.pradhan@minerra.net on 2022-06-29\n",
# 			"label": null,
# 			"maturity": "medium",
# 			"meta": {},
# 			"tags": [],
# 			"config": {
# 				"enabled": true
# 			},
# 			"unrendered_config": {},
# 			"url": "https://yellowfin-dev-ihsopk.6clicks.io/logoff.i4",
# 			"depends_on": {
# 				"macros": [],
# 				"nodes": [
# 					"model.sixclicks_dev_orig.vwAbpUser",
# 					"model.sixclicks_dev_orig.vwAbpUser",
# 					"model.sixclicks_dev_orig.vwAttestationAttestors",
# 					"model.sixclicks_dev_orig.vwAttestationItems",
# 					"model.sixclicks_dev_orig.vwAttestationOwners",
# 					"model.sixclicks_dev_orig.vwAttestations",
# 					"model.sixclicks_dev_orig.vwAttestorApprovals"
# 				]
# 			},
# 			"refs": [
# 				[
# 					"vwAbpUser"
# 				],
# 				[
# 					"vwAbpUser"
# 				],
# 				[
# 					"vwAttestationAttestors"
# 				],
# 				[
# 					"vwAttestationItems"
# 				],
# 				[
# 					"vwAttestationOwners"
# 				],
# 				[
# 					"vwAttestations"
# 				],
# 				[
# 					"vwAttestorApprovals"
# 				]
# 			],
# 			"sources": [],
# 			"created_at": 1682394414.4026499
# 		}
# ```

# In[ ]:


exposures_obj = pd.DataFrame({ 
    "id": [ n for n in data["exposures"].keys() ],
    "depends_on" : [ n["depends_on"] for n in data["exposures"].values() ],
    "materialized" : ["exposure"] * len(data["exposures"].keys()),
    "original_file_path" : [ n["original_file_path"] for n in data["exposures"].values() ],
    "name" : [ n["name"] for n in data["exposures"].values() ] 
    })
# exposures_obj


# ### 6. Union all into obj_df

# In[ ]:


obj_df = pd.concat([nodes_obj, sources_obj, macros_obj, docs_obj, exposures_obj], ignore_index=True)
#obj_df


# ## 2. Build the Tree

# ### 1. Table of nodes
# 

# In[ ]:


nodes_df = pd.DataFrame({ 
    "parent": [ n for n in data["parent_map"].keys() ],
    "child" : [ n for n in data["parent_map"].values() ] 
    })


# ### 2. Unnest the child column to get one row for each parent and child

# In[ ]:


nodes_unnest_df= nodes_df.explode("child", ignore_index=True)


# ### 3. Filter only exposure and models as parent and models as children

# In[ ]:


selected_nodes_unnest_df = nodes_unnest_df.    query("parent.str.contains('^model|exposure.*',regex=True)").    query("child == child").    query("child.str.contains('^model|source.*',regex=True)")


# ### 4. List of Root is the unique list of Exposures

# In[ ]:


root_list = list(set(selected_nodes_unnest_df.    query('parent.str.contains("^exposure.*")')["parent"].    tolist()))


# ### 5. Build root_df from root_list

# In[ ]:


root_df = pd.DataFrame({ 
    "parent" : ["ROOT"] * len(root_list),
    "child" : root_list 
    })
# root_df


# ### 6. Tree is union of the root and the rest of the tree

# In[ ]:


tree_df = pd.concat([selected_nodes_unnest_df,root_df], ignore_index=True)


# In[ ]:


tree_df


# ## 3. 
# ### 1. Mark parent column type in parent_group column
# 
# Types
# 1. root
# 2. middle
# 3. base_view (model has only one child which is a source)

# In[ ]:


# base_view (model has only one child which is a source) 
tree_df["parent_group"] = np.where((tree_df["parent"].str.contains("^model.*") & tree_df["child"].str.contains("^source.*")),"base_view" ,"")
# root
tree_df["parent_group"] = np.where((tree_df["parent"].str.contains("^ROOT")),"root" ,tree_df["parent_group"])
# middle
tree_df["parent_group"] = np.where((tree_df["parent_group"] == ""),"middle" ,tree_df["parent_group"])

tree_df


# ### 2. Traverse the tree and do stuff

# Traverse tree and mark the obj_df and tree_df

# In[ ]:


obj_df["notes"] = ""       # mark * if traverse tree visits the node
obj_df["pass_count"] = 0   # counts how many visits were made by traverse (How many references were made)
obj_df


# In[ ]:


tree_df["pass_count"] = 0


# Define functions

# In[ ]:


def mark_obj(id,note):
    global obj_df

    obj_df.loc[obj_df["id"] == id, "notes"] = note
    obj_df.loc[obj_df["id"] == id, "pass_count"] += 1
    tree_df.loc[tree_df["parent"] == id, "pass_count"] += 1


# In[ ]:



def traverse(node:str, level=0, silent=False):
    if not silent:
        print("\t"*level,node)
    mark_obj(node,"*")
    children_df = tree_df[tree_df["parent"].apply(lambda x: x == node)]
    if len(children_df["child"].tolist()) > 0:
        for child_node in children_df["child"]:
            traverse(child_node, level+1)  
    else:
        if not silent:
            print("-------------")


# Traverse the tree

# In[ ]:


traverse("ROOT", silent = True) # set to False to see the path thru the tree for debugging


# Orphaned objects
# 1. pass_count = 0
# 2. Is not a base_view - we do not delete any base views
# 3. Ignore Archived for Deletion folder

# In[ ]:


orphaned_views = list(set(tree_df.query("pass_count == 0 & parent_group != 'base_view'")["parent"].tolist() ))


# form the shell command

# In[ ]:


# Construct the columns to make the shell script
move_df = obj_df[obj_df["id"].isin(orphaned_views)][["id","name","original_file_path"]]
move_df["move_to"] = "models/Archive_for_deletion"
# Ignore unreferenced files in Archive folder already
move_df = move_df.query('~original_file_path.str.contains("Archive_for_deletion")')
move_df


# In[ ]:


# Sheel script string
move_df["shell"] = "mv " + move_df["original_file_path"] + " " + move_df["move_to"]
move_df["shell"]


# Create shell script

# In[ ]:


os.getcwd()


# In[ ]:


move_df["shell"].to_csv("archive_orphans.sh", index=False, header=False)


# Change directory to projects root folder and run the script

# In[ ]:


#os.chdir("..")
#!chmod +x target/archive_orphans.sh
#!bash target/archive_orphans.sh

