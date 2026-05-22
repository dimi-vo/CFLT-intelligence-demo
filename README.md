# Streaming LLM

This is a place to test out the latest Confluent Intelligence features.

The end-to-end demos where you run a shell script and suddenly a complete pipeline is up and running tell me nothing. If I don't know what exactly is in the box, how am I supposed to take that feature to production?

## What is Confluent Intelligence and what is in this repo

Here's what is in Confluent Intelligence

1. Streaming Agents: I do use them in this repo
2. Real Time Context Engine: This was released couple days ago, so I didn't have the time to test it out yet
3. Fully Managed MCP Server: Same as above
4. Built In ML functions: We do use these as well – see the last Flink statement.

What I've also done here is run a local MCP server that I expose with ngrok. What the MCP server does, is not of importance here. It can be replaced with an MCP server that actually does something useful.

Do not get confused with the fully managed MCP server that Confluent released. That one is for agents to interact with Confluent Cloud, e.g. read from a topic, fetch a schema etc. What I wanted to do is run an _external/self-managed_ MCP server that would represent a different service and that a Streaming Agent that is deployed on Confluent Cloud could reach.

I am using Claude hosted by Anthropic. Gemini and OpenAI models are also supported. [This list](https://docs.confluent.io/cloud/current/flink/reference/statements/create-connection.html#description) has all the supported connections.

## Prerequisites

- Python 3.12+
- [Terraform](https://www.terraform.io/) >= 1.0
- [uv](https://docs.astral.sh/uv/)
- [ngrok](https://ngrok.com/) (to expose the MCP server)
- [Confluent Cloud account](https://confluent.cloud/) with an API key and secret

## Getting Started

### 1. Provision Infrastructure

```bash
cd infrastructure
cp terraform.tfvars.example terraform.tfvars   # fill in your CC API key/secret
terraform init
terraform plan
terraform apply
```

This creates: a Confluent Cloud environment, a standard Kafka cluster, topics (`ohlcv_data`, `news`, `enriched_news`, `purchases`), service accounts with ACLs, Schema Registry, a DataGen source connector, and a Flink compute pool.

### 2. Configure the Producer

```bash
cd data-generation

cd ../infrastructure
terraform output -raw dotenv > ../data-generation/.env
```

### 3. Install Dependencies and Run the Producer

```bash
cd data-generation
uv sync

# Produce OHLCV stock data to 'ohlcv_data' topic
uv run python producer.py
```

The producer runs in a continuous loop — press `Ctrl+C` to stop. It fetches real historical daily data from Yahoo Finance and replays it one day per second.

### 4. Run & Expose the MCP Server

```bash
cd mcp-server/stock
uv sync
FASTMCP_HOST=0.0.0.0 MCP_TRANSPORT=sse uv run python main.py
```

In a separate terminal, expose it publicly:

```bash
ngrok http 8000
```

> Take note of the URL that ngrok is exposing your localhost:8000. Use the ngrok HTTPS URL in the `stock_api` connection below.

The server requires HTTP Basic Auth (defaults: `bob` / `whoisthat`, override via `MCP_USERNAME` / `MCP_PASSWORD`).

## Confluent Intelligence

Flink SQL statements to set up the Anthropic model connection, MCP server tool, and streaming agent. Run these in the Confluent Cloud Flink console.

### Anthropic demo

Open a workspace on Confluent Cloud and run the following queries.

```sql
-- Create a connection to the Anthropic API
-- https://docs.confluent.io/cloud/current/flink/reference/statements/create-connection.html
CREATE CONNECTION `anthropic_conn`
WITH (
    'type'     = 'anthropic',
    'endpoint' = 'https://api.anthropic.com/v1/messages',
    'api-key'  = '<YOUR_ANTHROPIC_API_KEY>'
);

-- Register a remote AI model using the connection above
-- https://docs.confluent.io/cloud/current/flink/reference/statements/create-model.html
CREATE MODEL `anthropic-model`
    INPUT (`text` VARCHAR(2147483647))
    OUTPUT (`output` VARCHAR(2147483647))
    WITH (
        'provider'                    = 'anthropic',
        'anthropic.connection'        = 'anthropic_conn',
        'anthropic.params.max_tokens' = '2048',
        'task'                        = 'text_generation'
    );

-- Create a simple input table to feed the agent
CREATE TABLE `demo-ticker-input` (
    `ticker`   STRING,
    `duration` INT,
    `interval` STRING
);

INSERT INTO `demo-ticker-input`
VALUES
    ('AAPL', 1, 'week'),
    ('NVDA', 2, 'weeks');

-- Connect to the self-managed MCP server (exposed via ngrok)
CREATE CONNECTION stock_api
WITH (
    'type'     = 'mcp_server',
    'endpoint' = '<YOUR_NGROK_URL>',
    'username' = 'bob',
    'password' = 'whoisthat'
);

-- Register an MCP tool that the agent can call
-- https://docs.confluent.io/cloud/current/flink/reference/statements/create-tool.html
CREATE TOOL check_vwap_for_ticker
USING CONNECTION stock_api
WITH (
    'type'            = 'mcp',
    'allowed_tools'   = 'check_vwap_confirmation',
    'request_timeout' = '30'
);

-- Define a streaming agent with a model, prompt, and tools
-- https://docs.confluent.io/cloud/current/flink/reference/statements/create-agent.html
CREATE AGENT stock_agent
USING MODEL `anthropic-model`
USING PROMPT 'You are a financial assistant. You start every sentence with "Hey Buddy, lemme check that out. So ...". The prompt will be about a stock and a time period. You need to calculate the VWAP of that ticker for the asked period. Use the corresponding tool'
USING TOOLS check_vwap_for_ticker
WITH (
    'tokens_management_strategy' = 'summarize',
    'max_tokens_threshold'       = '80000',
    'summarization_prompt'       = 'concise',
    'handle_exception'           = 'fail',
    'max_consecutive_failures'   = '1',
    'max_iterations'             = '3',
    'request_timeout'            = '60'
);

-- Run the agent against the input table
-- https://docs.confluent.io/cloud/current/flink/reference/functions/model-inference-functions.html#flink-sql-ai-run-agent-function
SELECT
    s.`ticker`,
    a.status,
    a.response
FROM `demo-ticker-input` AS s,
LATERAL TABLE (
    AI_RUN_AGENT(
        'stock_agent',
        'Check the vwap for the AAPL stock. Lookback is 3 days, 7 days, and 14 days'
    )
) AS a(status, response);

-- Use the built-in ML_FORECAST function on OHLCV data from the topic
-- Requires running the producer first: cd data-generation && uv run python producer.py
-- https://docs.confluent.io/cloud/current/ai/builtin-functions/forecast.html
SELECT
    ticker,
    ts,
    t_close,
    fc.forecast_value AS forecast
FROM (
    SELECT
        ticker,
        ts,
        t_close,
        ML_FORECAST(
            t_close,
            ts,
            JSON_OBJECT('minTrainingSize' VALUE 10, 'horizon' VALUE 1)
        ) OVER (
            PARTITION BY ticker
            ORDER BY ts
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS forecast_arr
    FROM ohlcv_data
) CROSS JOIN UNNEST(forecast_arr) AS fc;
```

## Todo

- [ ] Function call Agent — how to call an agent from another agent; eliminates manual SQL operator chaining for dynamic reasoning loops
- [ ] Include [agent logs](https://docs.confluent.io/cloud/current/ai/streaming-agents/monitor-streaming-agents.html#related-content)
