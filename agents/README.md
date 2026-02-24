# Ketchup Agents

`agents/planning.py` is the only supported orchestration surface.

## Status

- Canonical: `planning.py`

## Planner Capabilities

- OpenAI-compatible chat completions via `VLLM_BASE_URL`
- Tool-calling with:
  - `search_places` (Google Places API New)
  - `get_directions` (Google Routes API)
  - `web_search` (Tavily, optional)
- Deterministic synthesis fallback paths when model output is unusable
- Novelty controls (separate defaults for generate vs refine)
- Refine steering via descriptors and optional lead note
- Analytics-aware prompting and fallback ranking via materialized Postgres priors

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `VLLM_BASE_URL` | `http://localhost:8080/v1` | OpenAI-compatible endpoint |
| `VLLM_MODEL` | `Qwen/Qwen3-4B-Instruct-2507` | Model name in completion requests |
| `VLLM_API_KEY` | `EMPTY` | API key for model endpoint |
| `GOOGLE_MAPS_API_KEY` | empty | Enables maps tools |
| `TAVILY_API_KEY` | empty | Enables web-search fallback |
| `PLANNER_FALLBACK_ENABLED` | `false` | Enables generic non-grounded fallback |

## vLLM Tool-Calling Requirement

For tool-calling with vLLM, start server with:
- `--enable-auto-tool-choice`
- `--tool-call-parser hermes` (or parser matching your model/template)

## Verification

```bash
docker compose -f ketchup-local/docker-compose.yml exec -T backend env PYTHONPATH=/app \
  python -c "import asyncio,json; import agents.planning as planning; out=asyncio.run(planning._web_search(query='group activities for friends', location='Boston, MA', max_results=3)); print('ERROR:', out.get('error')); print('RESULT_COUNT:', len(out.get('results', []))); print(json.dumps(out.get('results', [])[:2], indent=2))"
```
