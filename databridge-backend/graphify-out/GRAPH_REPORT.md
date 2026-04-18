# Graph Report - /Users/pushkar/Documents/apps/databridge/databridge-backend  (2026-04-18)

## Corpus Check
- 20 files · ~98,658 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 94 nodes · 130 edges · 15 communities detected
- Extraction: 82% EXTRACTED · 18% INFERRED · 0% AMBIGUOUS · INFERRED: 23 edges (avg confidence: 0.67)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]
- [[_COMMUNITY_Community 9|Community 9]]
- [[_COMMUNITY_Community 10|Community 10]]
- [[_COMMUNITY_Community 11|Community 11]]
- [[_COMMUNITY_Community 12|Community 12]]
- [[_COMMUNITY_Community 13|Community 13]]
- [[_COMMUNITY_Community 14|Community 14]]

## God Nodes (most connected - your core abstractions)
1. `_process_import()` - 13 edges
2. `NocoDBClient` - 11 edges
3. `create_table()` - 7 edges
4. `_serialize_job()` - 7 edges
5. `Settings` - 6 edges
6. `parse_response_json()` - 5 edges
7. `ImportJob` - 5 edges
8. `ImportError` - 5 edges
9. `Synchronous wrapper for Celery to call the async import process.` - 5 edges
10. `parse_json_response()` - 5 edges

## Surprising Connections (you probably didn't know these)
- `create_table()` --calls--> `upgrade()`  [INFERRED]
  /Users/pushkar/Documents/apps/databridge/databridge-backend/app/routers/nocodb.py → /Users/pushkar/Documents/apps/databridge/databridge-backend/alembic/versions/0b7abe2958e6_init_db.py
- `CreateTableSchema` --uses--> `NocoDBClient`  [INFERRED]
  /Users/pushkar/Documents/apps/databridge/databridge-backend/app/routers/nocodb.py → /Users/pushkar/Documents/apps/databridge/databridge-backend/app/services/nocodb.py
- `create_job()` --calls--> `ImportJob`  [INFERRED]
  /Users/pushkar/Documents/apps/databridge/databridge-backend/app/routers/jobs.py → /Users/pushkar/Documents/apps/databridge/databridge-backend/app/models/job.py
- `ImportError` --calls--> `_process_import()`  [INFERRED]
  /Users/pushkar/Documents/apps/databridge/databridge-backend/app/models/job.py → /Users/pushkar/Documents/apps/databridge/databridge-backend/app/workers/import_task.py
- `Synchronous wrapper for Celery to call the async import process.` --uses--> `NocoDBClient`  [INFERRED]
  /Users/pushkar/Documents/apps/databridge/databridge-backend/app/workers/import_task.py → /Users/pushkar/Documents/apps/databridge/databridge-backend/app/services/nocodb.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.18
Nodes (11): get_db(), _get_resume_offset(), _json_safe_value(), _process_import(), _refresh_job(), _resolve_upload_path(), run_import_job(), _set_progress() (+3 more)

### Community 1 - "Community 1"
Cohesion: 0.18
Nodes (11): Base, BaseModel, Synchronous wrapper for Celery to call the async import process., ImportError, ImportJob, create_job(), JobCreateRequest, CreateTableSchema (+3 more)

### Community 2 - "Community 2"
Cohesion: 0.36
Nodes (9): create_table(), list_bases(), list_tables(), noco_error_detail(), parse_response_json(), raise_for_noco_status(), sanitize_column_name(), sanitize_table_name() (+1 more)

### Community 3 - "Community 3"
Cohesion: 0.25
Nodes (7): get_file_preview(), Reads the first `rows` of a file to return the headers and sample data     for t, Handle chunked file upload to bypass size limits., Return headers and first 20 rows of uploaded file., upload_chunk(), upload_preview(), UploadResponse

### Community 4 - "Community 4"
Cohesion: 0.46
Nodes (7): cancel_job(), get_job(), get_job_progress(), _get_live_progress(), list_jobs(), resume_job(), _serialize_job()

### Community 5 - "Community 5"
Cohesion: 0.4
Nodes (4): Run migrations in 'offline' mode.      This configures the context with just a U, Run migrations in 'online' mode.      In this scenario we need to create an Engi, run_migrations_offline(), run_migrations_online()

### Community 6 - "Community 6"
Cohesion: 0.5
Nodes (3): BaseSettings, Config, Settings

### Community 7 - "Community 7"
Cohesion: 0.5
Nodes (0): 

### Community 8 - "Community 8"
Cohesion: 0.5
Nodes (2): Init db  Revision ID: 0b7abe2958e6 Revises:  Create Date: 2026-04-12 19:30:10.42, upgrade()

### Community 9 - "Community 9"
Cohesion: 0.5
Nodes (1): Add webhook settings  Revision ID: a7e2c9f4a6b1 Revises: 5a18d7cb7c2a Create Dat

### Community 10 - "Community 10"
Cohesion: 0.5
Nodes (1): Add settings base id  Revision ID: 5a18d7cb7c2a Revises: 0b7abe2958e6 Create Dat

### Community 11 - "Community 11"
Cohesion: 1.0
Nodes (0): 

### Community 12 - "Community 12"
Cohesion: 1.0
Nodes (0): 

### Community 13 - "Community 13"
Cohesion: 1.0
Nodes (0): 

### Community 14 - "Community 14"
Cohesion: 1.0
Nodes (0): 

## Knowledge Gaps
- **9 isolated node(s):** `Config`, `Handle chunked file upload to bypass size limits.`, `Return headers and first 20 rows of uploaded file.`, `Reads the first `rows` of a file to return the headers and sample data     for t`, `Run migrations in 'offline' mode.      This configures the context with just a U` (+4 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 11`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 12`** (1 nodes): `celery_app.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 13`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 14`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `CreateTableSchema` connect `Community 1` to `Community 0`, `Community 2`?**
  _High betweenness centrality (0.188) - this node is a cross-community bridge._
- **Why does `NocoDBClient` connect `Community 0` to `Community 1`?**
  _High betweenness centrality (0.134) - this node is a cross-community bridge._
- **Why does `JobCreateRequest` connect `Community 1` to `Community 4`?**
  _High betweenness centrality (0.132) - this node is a cross-community bridge._
- **Are the 6 inferred relationships involving `_process_import()` (e.g. with `.close()` and `NocoDBClient`) actually correct?**
  _`_process_import()` has 6 INFERRED edges - model-reasoned connections that need verification._
- **Are the 4 inferred relationships involving `NocoDBClient` (e.g. with `CreateTableSchema` and `Synchronous wrapper for Celery to call the async import process.`) actually correct?**
  _`NocoDBClient` has 4 INFERRED edges - model-reasoned connections that need verification._
- **Are the 4 inferred relationships involving `Settings` (e.g. with `CreateTableSchema` and `TablePreset`) actually correct?**
  _`Settings` has 4 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Config`, `Handle chunked file upload to bypass size limits.`, `Return headers and first 20 rows of uploaded file.` to the rest of the system?**
  _9 weakly-connected nodes found - possible documentation gaps or missing edges._