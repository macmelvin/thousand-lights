# Thousand Lights

A shared 3D house of prayer. Visitors pick their faith (church, Catholic church, mosque, temple, mandir, synagogue, gurdwara or an open chapel), walk around as a small figure, see who else is there live, lift prayers that float as lights, tap "I prayed" for others, and get a reply from an AI prayer companion that answers with scripture, a short prayer and encouragement. The companion is a guide, never God.

## Run locally

```
npm install
npm start            # http://localhost:3000
```

## Environment variables

| Variable | Needed | What it does |
|---|---|---|
| `ANTHROPIC_API_KEY` | one of these two | Turns on the companion with Claude (`ANTHROPIC_MODEL`, default `claude-haiku-5-5`). Used first if set. |
| `OPENAI_API_KEY` | | Turns on the companion with OpenAI (`OPENAI_MODEL`, default `gpt-5-mini`). |
| `ADMIN_SECRET` | recommended | Lets you remove any prayer. Open `/#admin`, enter the secret, and Remove links appear on every prayer. |
| `DATA_DIR` | on Railway | Where the SQLite database lives. Point it at a mounted volume, e.g. `/data`. |

## Limits built in

- Up to 5 prayers per device per 10 minutes (15 per IP).
- Up to 8 companion replies per device per hour (20 per IP), to keep AI costs predictable.
- Prayers are anonymous; people can remove only their own (tracked by a random device id in their browser).
