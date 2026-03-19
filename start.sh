#!/bin/bash
source /Users/dev-server/workspace/services/mapping_tools/.venv/bin/activate
cd /Users/dev-server/workspace/services/mapping_tools
exec uvicorn src.api:app --host 0.0.0.0 --port 8003
