# Phase 3 Research: OpenCode Go API + Envio HyperIndex for MegaETH

## 1. OpenCode Go API Endpoints

### 1.1 Base URL & OpenAI Compatibility

OpenCode Go exposes two API styles:

**OpenAI-Compatible** (`/chat/completions`):
- Base URL: `https://opencode.ai/zen/go/v1/chat/completions`
- Models: deepseek-v4-pro, deepseek-v4-flash, glm-5, glm-5.1, kimi-k2.5, kimi-k2.6, mimo-v2.5, mimo-v2.5-pro
- **Yes, `langchain-openai` ChatOpenAI works** — just set `openai_api_base` (or `base_url` in newer versions) to the endpoint.

```python
from langchain_openai import ChatOpenAI

llm = ChatOpenAI(
    model="deepseek-v4-flash",  # or "deepseek-v4-pro", "kimi-k2.6", etc.
    openai_api_key="opencode-go-<your-key>",
    openai_api_base="https://opencode.ai/zen/go/v1",
    temperature=0,
)
```

**Anthropic-Compatible** (`/messages`):
- Base URL: `https://opencode.ai/zen/go/v1/messages`
- Models: qwen3.7-max, qwen3.6-plus, minimax-m3, minimax-m2.7, minimax-m2.5
- Use `@ai-sdk/anthropic` compatible, or `langchain-anthropic` with base_url override:

```python
from langchain_anthropic import ChatAnthropic

llm = ChatAnthropic(
    model="qwen3.7-max",
    anthropic_api_key="opencode-go-<your-key>",
    anthropic_api_url="https://opencode.ai/zen/go/v1",
)
```

### 1.2 Available Models (beyond deepseek-v4-flash)

| Model | Endpoint Type | Model ID for config |
|-------|---------------|---------------------|
| GLM-5.1 | `/chat/completions` | `opencode-go/glm-5.1` |
| GLM-5 | `/chat/completions` | `opencode-go/glm-5` |
| Kimi K2.5 | `/chat/completions` | `opencode-go/kimi-k2.5` |
| Kimi K2.6 | `/chat/completions` | `opencode-go/kimi-k2.6` |
| DeepSeek V4 Pro | `/chat/completions` | `opencode-go/deepseek-v4-pro` |
| DeepSeek V4 Flash | `/chat/completions` | `opencode-go/deepseek-v4-flash` |
| MiMo-V2.5 | `/chat/completions` | `opencode-go/mimo-v2.5` |
| MiMo-V2.5-Pro | `/chat/completions` | `opencode-go/mimo-v2.5-pro` |
| MiniMax M3 | `/messages` | `opencode-go/minimax-m3` |
| MiniMax M2.7 | `/messages` | `opencode-go/minimax-m2.7` |
| MiniMax M2.5 | `/messages` | `opencode-go/minimax-m2.5` |
| Qwen3.7 Max | `/messages` | `opencode-go/qwen3.7-max` |
| Qwen3.6 Plus | `/messages` | `opencode-go/qwen3.6-plus` |

### 1.3 Model Metadata Endpoint

```
GET https://opencode.ai/zen/go/v1/models
```

Returns full list of available models and their metadata. This is an OpenAI-compatible `/models` endpoint.

### 1.4 Authentication

- **Header**: `Authorization: Bearer opencode-go-<your-api-key>`
- Get the API key from [OpenCode Zen](https://opencode.ai/auth) after subscribing to Go.
- Subscription: **$5 first month, then $10/month**.

### 1.5 Rate Limits & Pricing

**Token pricing (per 1M tokens):**

| Model | Input | Output | Cached Read |
|-------|-------|--------|-------------|
| DeepSeek V4 Flash | $0.14 | $0.28 | $0.0028 |
| DeepSeek V4 Pro | $1.74 | $3.48 | $0.0145 |
| Kimi K2.5 | $0.60 | $3.00 | $0.10 |
| Qwen3.7 Max | $2.50 | $7.50 | $0.50 |
| MiMo-V2.5 | $0.14 | $0.28 | $0.0028 |

**Usage limits (dollar-based):**
- 5-hour: $12
- Weekly: $30
- Monthly: $60

**Estimated requests per month:**
- DeepSeek V4 Flash: ~158,000 requests/mo (cheapest)
- DeepSeek V4 Pro: ~17,000 requests/mo
- Qwen3.7 Max: ~4,800 requests/mo (most expensive)
- Kimi K2.5: ~9,300 requests/mo

**Beyond limits:** If you have a Zen balance, enable "Use balance" to fall back to credits instead of blocking.

### 1.6 Key Findings for Phase 3

1. **OpenAI-compatible endpoint confirmed.** The `langchain-openai` ChatOpenAI class with `openai_api_base="https://opencode.ai/zen/go/v1"` will work for all `/chat/completions` models.
2. **Structured output support:** Since `/chat/completions` is OpenAI-compatible, `with_structured_output()` should work for models that support JSON mode / response_format.
3. **Caching is priced separately** — cached reads are ~10x cheaper than regular input.
4. **Monthly cap of $60** means you get ~17K requests with DeepSeek V4 Pro (solid for LangGraph agent loops), or ~158K with Flash.
5. **No free tier for Go** — you must subscribe. But OpenCode itself works with any free provider.

---

## 2. Envio HyperIndex for MegaETH

### 2.1 MegaETH Support Status

**Officially supported.** All three MegaETH networks are on Envio's supported list:

| Network | Chain ID | HyperSync URL | HyperRPC URL |
|---------|----------|---------------|--------------|
| MegaETH Mainnet | 4326 | `https://megaeth.hypersync.xyz` or `https://4326.hypersync.xyz` | `https://megaeth.rpc.hypersync.xyz` or `https://4326.rpc.hypersync.xyz` |
| MegaETH Testnet | 6342 | `https://megaeth-testnet.hypersync.xyz` | `https://megaeth-testnet.rpc.hypersync.xyz` |
| MegaETH Testnet2 | 6343 | `https://megaeth-testnet2.hypersync.xyz` | `https://megaeth-testnet2.rpc.hypersync.xyz` |

### 2.2 Blueprint Indexer Setup for MegaETH

**Initialize an indexer:**

```bash
pnpx envio init
```

Select "Contract Import" → choose "megaeth-mainnet" (or "megaeth-testnet2" for chain ID 6343) → enter your contract address.

**config.yaml example for MegaETH DEX indexing:**

```yaml
name: megaeth-dex-indexer
networks:
  - id: 4326  # MegaETH mainnet
    start_block: 0
    contracts:
      - name: DEX
        handler: src/EventHandlers.ts
        events:
          - event: Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)
          - event: Transfer(indexed address from, indexed address to, uint256 value)
          - event: Sync(uint112 reserve0, uint112 reserve1)
    rpc_config:
      url: https://4326.rpc.hypersync.xyz
```

### 2.3 Deployed GraphQL Endpoint URL Format

When deployed to Envio Cloud, the GraphQL API endpoint follows this pattern:

```
https://indexer.envio.dev/v1/subgraph/{org}/{indexer-name}/graphql
```

For example, if your org is `megaeth-analytics` and indexer is `dex-v2`:

```
https://indexer.envio.dev/v1/subgraph/megaeth-analytics/dex-v2/graphql
```

This is a production-grade, hosted GraphQL endpoint.

### 2.4 Authentication for Envio Endpoints

**HyperSync API token:** Required for all HyperSync requests (both local dev and self-hosted). Generate at [Envio Cloud Portal](https://envio.dev/app/api-tokens).

- **Local development:** Set `ENVIO_API_TOKEN` in `.env` file.
- **Envio Cloud deployments:** No token needed — special access is provided.
- **GraphQL endpoint (hosted):** May require IP/domain whitelisting (configurable in Envio Cloud dashboard). No API key header by default.

### 2.5 What Data Is Indexable

**Any EVM event** from any smart contract on MegaETH:
- **DEX swaps, transfers, pools** — via contract event indexing
- **ERC-20 / ERC-721 / ERC-1155** transfers and approvals
- **Lending protocol** deposits, withdrawals, liquidations
- **Any custom event** defined in any smart contract ABI

Key capabilities:
- **Wildcard indexing** — index all events matching a topic signature without pre-registering contracts
- **Factory contracts** — support for 1M+ dynamically registered contracts
- **Contract state** — can call `eth_call` on contracts to read on-chain state during indexing
- **Multichain** — a single indexer can span multiple chains
- **Reorg support** — handles MegaETH's mini-block reorganizations

### 2.6 GraphQL Schema Pattern

Envio auto-generates entities from your `schema.graphql` file. Here's an example for a DEX indexer:

**schema.graphql:**

```graphql
type Swap {
  id: ID!
  sender: String!
  amount0In: BigInt!
  amount1In: BigInt!
  amount0Out: BigInt!
  amount1Out: BigInt!
  to: String!
  timestamp: BigInt!
}

type Pool {
  id: ID!
  token0: String!
  token1: String!
  reserve0: BigInt!
  reserve1: BigInt!
  totalSwaps: BigInt!
  volumeUSD: BigFloat!
}

type TokenDayData {
  id: ID!
  token: String!
  date: Int!
  priceUSD: BigFloat!
  volumeUSD: BigFloat!
  totalValueLockedUSD: BigFloat!
}
```

**Common queries against the hosted endpoint:**

```graphql
# Recent swaps
query RecentSwaps($limit: Int!) {
  Swap(
    limit: $limit,
    order_by: { timestamp: desc }
  ) {
    id
    sender
    amount0In
    amount1In
    amount0Out
    amount1Out
    timestamp
  }
}

# Pool stats
query PoolStats($limit: Int!) {
  Pool(
    limit: $limit,
    order_by: { volumeUSD: desc }
  ) {
    id
    token0
    token1
    reserve0
    reserve1
    totalSwaps
    volumeUSD
  }
}

# Daily token data (time-series)
query TokenDayData($token: String!, $days: Int!) {
  TokenDayData(
    limit: $days,
    order_by: { date: desc },
    where: { token: { _eq: $token } }
  ) {
    date
    priceUSD
    volumeUSD
    totalValueLockedUSD
  }
}
```

### 2.7 Python Integration for Phase 3

Use the `gql` library (already in `pyproject.toml`) to query Envio's GraphQL endpoint:

```python
"""Query Envio HyperIndex GraphQL endpoint from Python."""
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

ENVIO_GRAPHQL_URL = (
    "https://indexer.envio.dev/v1/subgraph/"
    "megaeth-analytics/dex-v2/graphql"
)

async def fetch_recent_swaps(limit: int = 100) -> list[dict]:
    transport = AIOHTTPTransport(url=ENVIO_GRAPHQL_URL)
    async with Client(transport=transport) as session:
        query = gql("""
            query RecentSwaps($limit: Int!) {
                Swap(limit: $limit, order_by: { timestamp: desc }) {
                    id
                    sender
                    amount0In
                    amount1In
                    amount0Out
                    amount1Out
                    timestamp
                }
            }
        """)
        result = await session.execute(query, variable_values={"limit": limit})
        return result["Swap"]
```

### 2.8 Key Differences: Envio vs The Graph

| Feature | Envio HyperIndex | The Graph |
|---------|-----------------|-----------|
| Speed | 30,000+ events/sec | ~200 events/sec |
| MegaETH Support | Native (4326, 6342, 6343) | Unknown/Unverified |
| Multichain | Built-in single indexer | Separate subgraphs |
| Contract imports | Auto-generate from ABI/explorer | Manual schema writing |
| Hosted endpoint | `indexer.envio.dev` | `api.thegraph.com` |
| Reorg support | Built-in | Manual |
| API Token | Required for HyperSync | Not required for hosted |
| Language | TypeScript, JavaScript, ReScript | AssemblyScript, Rust |

### 2.9 Action Items for Phase 3

1. **Create Envio indexer** for key MegaETH DEX contracts (Kyber, any AMM pools).
2. **Host the indexer on Envio Cloud** to get a stable GraphQL endpoint URL.
3. **Replace The Graph URL in `query_historical_subgraph` tool** with the Envio endpoint.
4. **Wire the Envio GraphQL endpoint** into the LangGraph Market Analysis agent as a `@tool`.
5. **Set up HyperSync RPC** (`https://4326.rpc.hypersync.xyz`) as a fallback/alternative RPC for faster block data access.
6. **Register org/indexer names** in Envio Cloud to get a clean endpoint URL.
