# Control API MVP

Step 8 adds a lightweight API for inspecting and updating scenario configuration.

The API does not control producer or consumer runtime behavior yet. Later steps wire these settings into the services.

## Run

```bash
npm run start -w apps/control-api
```

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Service health. |
| `GET` | `/status` | Service uptime and scenario state. |
| `GET` | `/scenarios` | Current scenario configuration. |
| `PATCH` | `/scenarios` | Merge scenario configuration updates. |

## Example

```bash
curl http://localhost:3000/health
```

```bash
curl -X PATCH http://localhost:3000/scenarios \
  -H "content-type: application/json" \
  -d '{"consumerLag":{"enabled":true,"consumerDelayMs":500}}'
```

## Step 8 Verification

```bash
npm run check -w apps/control-api
npm run test
```
