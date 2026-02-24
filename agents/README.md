# Ketchup Planner Agent

`agents/planning.py` is the canonical LLM orchestration surface for plan generation and refine.

## Responsibilities

- Calls OpenAI-compatible chat completions endpoint (`VLLM_BASE_URL`).
- Executes tool loop with planner tools.
- Enforces strict plan schema parsing.
- Falls back to deterministic grounded synthesis when model output is invalid or empty.
- Applies novelty controls for generate vs refine.
- Uses analytics priors/snapshots from Postgres.

## Tools

- `search_places` (Google Places API)
- `get_directions` (Google Routes API)
- `web_search` (Tavily, optional fallback when map results are insufficient)

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `VLLM_BASE_URL` | `http://localhost:8080/v1` | OpenAI-compatible model endpoint |
| `VLLM_MODEL` | `Qwen/Qwen3-4B-Instruct-2507` | Model ID passed in chat requests |
| `VLLM_API_KEY` | `EMPTY` | Endpoint auth key |
| `GOOGLE_MAPS_API_KEY` | empty | Enables maps tools |
| `TAVILY_API_KEY` | empty | Enables web search tool |
| `PLANNER_NOVELTY_TARGET_GENERATE` | `0.7` | Diversity target for new rounds |
| `PLANNER_NOVELTY_TARGET_REFINE` | `0.35` | Diversity target for refinement |
| `PLANNER_FALLBACK_ENABLED` | `false` | Enables non-grounded generic fallback |

## vLLM Tool-Calling Requirement

For automatic tool calling in vLLM, run server with:

- `--enable-auto-tool-choice`
- `--tool-call-parser <parser-matching-model-template>`

If these flags are missing, planner logs tool-loop failure and uses deterministic grounded fallback.

## Verification

Web-search smoke test:

```bash
docker compose -f ketchup-local/docker-compose.yml exec -T backend env PYTHONPATH=/app python -c "import asyncio,json; import agents.planning as p; out=asyncio.run(p._web_search(query='group activities for friends', location='Boston, MA', max_results=3)); print('ERROR:', out.get('error')); print('RESULT_COUNT:', len(out.get('results', []))); print(json.dumps(out.get('results', [])[:2], indent=2))"
```
