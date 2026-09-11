# AI SDK chat, agent, multi-agent

Three [Vercel AI SDK](https://ai-sdk.dev/) apps on one Node server. Every message, tool step and agent turn is a record in a PicoMQ stream. Restart the server and the UI comes back from the streams.

Source: [`examples/agents/ai-sdk`](https://github.com/PicoMQ/picomq/tree/main/examples/agents/ai-sdk).

<div class="pico-diagram">
<svg viewBox="0 0 728 348" width="728" role="img" aria-label="A browser talks to a small Node server over fetch and SSE. The server calls OpenAI through the AI SDK and appends every message, step and turn to PicoMQ streams under one prefix. On start it lists the prefix and reads the streams back.">
  <defs>
    <marker id="asa" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <g transform="translate(225 6)">
    <rect x="-14" y="14" width="183" height="308" class="edge-soft"/>
<text x="-4" y="30" class="sub">node, tsx server/index.ts</text>
    <rect x="245" y="14" width="238" height="308" class="edge-soft"/>
<text x="255" y="30" class="sub">pico :4437</text>
    <rect x="-205" y="40" width="131" height="56" class="box"/>
<text x="-139" y="64" text-anchor="middle" class="label">browser :3456</text>
<text x="-139" y="82" text-anchor="middle" class="sub">fetch, SSE</text>
    <rect x="-205" y="112" width="131" height="56" class="box"/>
<text x="-139" y="136" text-anchor="middle" class="label">OpenAI</text>
<text x="-139" y="154" text-anchor="middle" class="sub">gpt-4o-mini</text>
    <rect x="0" y="40" width="155" height="56" class="box"/>
<text x="78" y="64" text-anchor="middle" class="label">chat.ts</text>
<text x="78" y="82" text-anchor="middle" class="sub">streamText</text>
    <rect x="0" y="112" width="155" height="56" class="box"/>
<text x="78" y="136" text-anchor="middle" class="label">agent.ts</text>
<text x="78" y="154" text-anchor="middle" class="sub">generateText, tools</text>
    <rect x="0" y="184" width="155" height="124" class="box"/>
<text x="78" y="208" text-anchor="middle" class="label">multi.ts</text>
<text x="78" y="226" text-anchor="middle" class="sub">3 agents, host</text>
    <rect x="259" y="40" width="210" height="56" class="box"/>
<text x="364" y="64" text-anchor="middle" class="label">chat/{id}</text>
<text x="364" y="82" text-anchor="middle" class="sub">one ModelMessage per record</text>
    <rect x="259" y="112" width="210" height="56" class="box"/>
<text x="364" y="136" text-anchor="middle" class="label">agent/run-{id}</text>
<text x="364" y="154" text-anchor="middle" class="sub">run_start, step, run_end</text>
    <rect x="259" y="184" width="210" height="56" class="box"/>
<text x="364" y="208" text-anchor="middle" class="label">multi/{room}/bus</text>
<text x="364" y="226" text-anchor="middle" class="sub">host and agent turns</text>
    <rect x="259" y="252" width="210" height="56" class="box"/>
<text x="364" y="276" text-anchor="middle" class="label">multi/{room}/agent/{id}</text>
<text x="364" y="294" text-anchor="middle" class="sub">memory per agent</text>
    <path d="M-74 68 L-18 68" class="edge" marker-end="url(#asa)"/>
    <path d="M-14 140 L-70 140" class="edge" marker-end="url(#asa)"/>
    <path d="M169 68 L255 68" class="edge" marker-end="url(#asa)"/>
    <path d="M169 140 L255 140" class="edge" marker-end="url(#asa)"/>
    <path d="M169 212 L255 212" class="edge" marker-end="url(#asa)"/>
    <path d="M169 280 L255 280" class="edge" marker-end="url(#asa)"/>
    <text x="214" y="60" text-anchor="middle" class="sub">append</text>
    <text x="214" y="132" text-anchor="middle" class="sub">append</text>
    <text x="214" y="204" text-anchor="middle" class="sub">append</text>
    <text x="214" y="272" text-anchor="middle" class="sub">append</text>
    <text x="-44" y="60" text-anchor="middle" class="sub">/api/*</text>
    <text x="-44" y="132" text-anchor="middle" class="sub">ai</text>
  </g>
</svg>
</div>

| Page | Streams | Records |
| --- | --- | --- |
| `/chat.html` | `chat/{id}`, one per conversation | AI SDK `ModelMessage` JSON |
| `/agent.html` | `agent/run-{id}`, one per run | `run_start`, `step`, `run_end` |
| `/multi.html` | `multi/{room}/bus`, `multi/{room}/agent/{ada,remy,quill}` | bus turns, per-agent memory |

All under prefix `/examples/agents/ai-sdk`. The server uses `@picomq/client` with protocol `pico` and a `Producer` per stream, `lingerMs: 10`. Each append waits for `durable()` before the seq is shown in the UI.

## Run

```bash
cd harness/aio
cp .env.example .env
docker compose up --build
```

```bash
export OPENAI_API_KEY=...
export PICO_ENDPOINT=http://127.0.0.1:4437

cd examples/agents/ai-sdk
npm install
npm run dev
```

Open `http://localhost:3456`.

## Chat persistence

- New chat creates `chat/{id}`. Each user and assistant message is appended as one record.
- Reply is `streamText` with `gpt-4o-mini`, streamed to the browser over SSE, then appended once complete.
- Context sent to the model is the last 40 messages, `AI_SDK_MAX_CONTEXT_MESSAGES`.
- On start the server lists the prefix and opens the newest stream. Recents are built from the first user message of each stream.
- Restart reads the stream from the beginning and replays it into the UI.

## Agent audit trail

<div class="pico-diagram">
<svg viewBox="0 0 679 112" width="679" role="img" aria-label="One run is one stream. run_start, one record per step with tool calls and results, then run_end.">
  <defs>
    <marker id="asb" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <g transform="translate(20 -10)">
    <rect x="0" y="30" width="99" height="56" class="box-accent"/>
<text x="50" y="54" text-anchor="middle" class="label">run_start</text>
<text x="50" y="72" text-anchor="middle" class="sub">prompt</text>
    <rect x="129" y="30" width="88" height="56" class="box"/>
<text x="173" y="54" text-anchor="middle" class="label">step 1</text>
<text x="173" y="72" text-anchor="middle" class="sub">lookupCompany</text>
    <rect x="247" y="30" width="88" height="56" class="box"/>
<text x="292" y="54" text-anchor="middle" class="label">step 2</text>
<text x="292" y="72" text-anchor="middle" class="sub">calculate</text>
    <rect x="366" y="30" width="88" height="56" class="box"/>
<text x="410" y="54" text-anchor="middle" class="label">step 3</text>
<text x="410" y="72" text-anchor="middle" class="sub">text</text>
    <rect x="484" y="30" width="155" height="56" class="box-accent"/>
<text x="562" y="54" text-anchor="middle" class="label">run_end</text>
<text x="562" y="72" text-anchor="middle" class="sub">text, steps, tokens</text>
    <path d="M99 58 L125 58" class="edge" marker-end="url(#asb)"/>
    <path d="M217 58 L243 58" class="edge" marker-end="url(#asb)"/>
    <path d="M336 58 L362 58" class="edge" marker-end="url(#asb)"/>
    <path d="M454 58 L480 58" class="edge" marker-end="url(#asb)"/>
    <text x="0" y="108" text-anchor="start" class="sub">seq 0</text>
    <text x="639" y="108" text-anchor="end" class="sub">seq 4, producer closed</text>
    <text x="292" y="108" text-anchor="middle" class="sub">one record per onStepFinish</text>
  </g>
</svg>
</div>

- `generateText` with tools `lookupCompany` and `calculate`, `stopWhen: stepCountIs(10)`.
- `onStepFinish` appends a `step` record with `text`, `toolCalls`, `toolResults`, `finishReason`.
- `run_end` carries the final text, step count and `totalTokens`. The producer is closed after it.
- Continuing a run reuses the stream if it is not closed. Prior `run_start` and `run_end` records rebuild the message history.

## Multi-agent persistence

<div class="pico-diagram">
<svg viewBox="0 0 702 308" width="702" role="img" aria-label="The host posts to the room bus. Each agent reads new bus records into its own memory stream, and when it is its turn the server runs generateText over that memory and posts the reply back to the bus.">
  <defs>
    <marker id="asc" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <g transform="translate(-17 -80)">
    <rect x="91" y="176" width="538" height="192" class="edge-soft"/>
<text x="101" y="358" class="sub">round robin, generateText over own memory</text>
    <rect x="37" y="100" width="142" height="56" class="box"/>
<text x="108" y="124" text-anchor="middle" class="label">host</text>
<text x="108" y="142" text-anchor="middle" class="sub">topic, follow-ups</text>
    <rect x="239" y="100" width="242" height="56" class="box-accent"/>
<text x="360" y="124" text-anchor="middle" class="label">multi/{room}/bus</text>
<text x="360" y="142" text-anchor="middle" class="sub">BusMessage {from, content, turn}</text>
    <rect x="105" y="190" width="150" height="56" class="box"/>
<text x="180" y="214" text-anchor="middle" class="label">Ada</text>
<text x="180" y="232" text-anchor="middle" class="sub">staff engineer</text>
    <rect x="285" y="190" width="150" height="56" class="box"/>
<text x="360" y="214" text-anchor="middle" class="label">Remy</text>
<text x="360" y="232" text-anchor="middle" class="sub">product lead</text>
    <rect x="465" y="190" width="150" height="56" class="box"/>
<text x="540" y="214" text-anchor="middle" class="label">Quill</text>
<text x="540" y="232" text-anchor="middle" class="sub">reviewer</text>
    <rect x="105" y="286" width="150" height="56" class="box"/>
<text x="180" y="310" text-anchor="middle" class="label">agent/ada</text>
<text x="180" y="328" text-anchor="middle" class="sub">memory</text>
    <rect x="285" y="286" width="150" height="56" class="box"/>
<text x="360" y="310" text-anchor="middle" class="label">agent/remy</text>
<text x="360" y="328" text-anchor="middle" class="sub">memory</text>
    <rect x="465" y="286" width="150" height="56" class="box"/>
<text x="540" y="310" text-anchor="middle" class="label">agent/quill</text>
<text x="540" y="328" text-anchor="middle" class="sub">memory</text>
    <path d="M179 128 L235 128" class="edge" marker-end="url(#asc)"/>
    <path d="M360 156 L360 176" class="edge"/>
<path d="M180 176 L540 176" class="edge"/>
<path d="M180 176 L180 186" class="edge" marker-end="url(#asc)"/>
<path d="M360 176 L360 186" class="edge" marker-end="url(#asc)"/>
<path d="M540 176 L540 186" class="edge" marker-end="url(#asc)"/>
    <path d="M615 218 L659 218 L659 128 L485 128" class="edge" marker-end="url(#asc)"/>
    <path d="M180 246 L180 282" class="edge" marker-end="url(#asc)"/>
    <path d="M360 246 L360 282" class="edge" marker-end="url(#asc)"/>
    <path d="M540 246 L540 282" class="edge" marker-end="url(#asc)"/>
    <text x="209" y="120" text-anchor="middle" class="sub">post</text>
    <text x="667" y="177" text-anchor="start" class="sub">turn</text>
    <text x="188" y="270" text-anchor="start" class="sub">user, assistant, busSeq</text>
  </g>
</svg>
</div>

- The host posts a topic to the bus. Every agent memory gets `[Host]: topic` as a `user` record with the bus seq.
- Advance runs `generateText` for the next agent over its own memory stream, posts the reply to the bus, appends it to its memory as `assistant` and to the others as `user`.
- Each memory record stores `busSeq`. On reload the server compares it with the bus and appends anything an agent has not seen.
- Delete removes the bus and the three memory streams.

## Environment

| Variable | Default |
| --- | --- |
| `PICO_ENDPOINT` | `http://127.0.0.1:4437` |
| `PORT` | `3456` |
| `OPENAI_API_KEY` | required |
| `AI_SDK_MAX_CONTEXT_MESSAGES` | `40` |
| `PICO_CHAT_PREFIX` | `/examples/agents/ai-sdk/chat` |
| `PICO_AGENT_PREFIX` | `/examples/agents/ai-sdk/agent` |
| `PICO_MULTI_PREFIX` | `/examples/agents/ai-sdk/multi` |
