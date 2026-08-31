# AI SDK examples on PicoMQ

Needs Docker Pico (`harness/aio`, protocol `pico`) and an OpenAI key.

```bash
export OPENAI_API_KEY=...
export PICO_ENDPOINT=http://127.0.0.1:4437

cd examples/ai-sdk
npm install
npm run dev
```

Open http://localhost:3456

| Page | What it is |
|------|------------|
| [/chat](http://localhost:3456/chat.html) | Chat Persistence |
| [/agent](http://localhost:3456/agent.html) | Agent Audit Trail |
| [/multi](http://localhost:3456/multi.html) | Multi-agent Persistence |
