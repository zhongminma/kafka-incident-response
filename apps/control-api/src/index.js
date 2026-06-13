import express from "express";

const config = {
  port: Number.parseInt(process.env.PORT ?? "3000", 10),
  serviceName: process.env.SERVICE_NAME ?? "control-api"
};

const scenarioState = {
  consumerLag: {
    enabled: false,
    producerIntervalMs: 1000,
    consumerDelayMs: 0
  },
  brokerFailure: {
    expected: false,
    notes: "Use docker compose stop kafka to simulate broker failure."
  },
  duplicates: {
    enabled: false,
    duplicateEventId: null
  },
  poisonMessage: {
    enabled: false,
    mode: null
  }
};

const app = express();
app.use(express.json());

app.get("/health", (_request, response) => {
  response.json({
    service: config.serviceName,
    status: "ok",
    timestamp: new Date().toISOString()
  });
});

app.get("/status", (_request, response) => {
  response.json({
    service: config.serviceName,
    uptimeSeconds: Math.round(process.uptime()),
    scenarios: scenarioState
  });
});

app.get("/scenarios", (_request, response) => {
  response.json(scenarioState);
});

app.patch("/scenarios", (request, response) => {
  const patch = request.body;

  if (!patch || typeof patch !== "object" || Array.isArray(patch)) {
    response.status(400).json({ error: "Request body must be an object." });
    return;
  }

  for (const [scenario, values] of Object.entries(patch)) {
    if (!Object.hasOwn(scenarioState, scenario)) {
      response.status(400).json({ error: `Unknown scenario: ${scenario}` });
      return;
    }

    if (!values || typeof values !== "object" || Array.isArray(values)) {
      response.status(400).json({ error: `Scenario patch must be an object: ${scenario}` });
      return;
    }

    scenarioState[scenario] = {
      ...scenarioState[scenario],
      ...values
    };
  }

  response.json(scenarioState);
});

app.use((request, response) => {
  response.status(404).json({
    error: "Not found",
    method: request.method,
    path: request.path
  });
});

app.listen(config.port, () => {
  console.log(`${config.serviceName} listening on port ${config.port}`);
});
