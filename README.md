# Kùzu Graph Explorer

A lightweight graph database browser built with **Streamlit** + **Kùzu** + **Pyvis**.

Upload a Kùzu database (or connect to a local path), then explore, query, and edit your graph — all from the browser.

## Features

- **Query** — Write Cypher or describe what you want in natural language (LLM-powered). Results render as interactive force-directed graphs.
- **Schema** — Visualize node tables, relationship tables, and their connections at a glance.
- **Edit** — Click nodes/edges to edit properties in-place. Drag lines between nodes to create relationships. Changes write directly to the database.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run
python -m streamlit run main.py
```

If the default PyPI source is slow, use a mirror:

```bash
pip install -r requirements.txt \
  --default-timeout=600 --retries=15 \
  -i https://mirrors.aliyun.com/pypi/simple/ \
  --trusted-host mirrors.aliyun.com
```

## Project Structure

```
├── main.py              # Streamlit app entry point & page routing
├── views.py             # Query results, schema view, edit canvas
├── visualization.py     # Pyvis graph generation & vis.js customization
├── graph.py             # DataFrame → graph node/edge extraction
├── db.py                # Kùzu connection, Cypher execution, write helpers
├── llm.py               # Natural language → Cypher via LLM
├── config.py            # Default settings (LLM endpoint, limits, etc.)
├── theme.py             # Streamlit CSS theme injection
├── session.py           # Session state helpers
├── uploads.py           # Database file upload handling
├── vis_bridge/          # Streamlit component for vis.js ↔ Python messaging
├── requirements.txt     # Python dependencies
└── LICENSE              # MIT License
```

## Requirements

- Python 3.10+
- Dependencies listed in `requirements.txt`

## License

MIT
