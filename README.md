# More Retail (Docs) GRN Scheduler

Pulls More Retail **GRN** documents out of Gmail every 3 hours, extracts the line items, and writes them to Google Sheets and Supabase.

## Pipeline

```
Gmail  --->  Google Drive  --->  LlamaParse  --->  Google Sheets
                                            \--->  Supabase
```

| Stage | What happens |
|---|---|
| **1. Gmail -> Drive** | Searches mail from `docs@more.in` matching `grn`, looking back 21 days, and saves each attachment to a Drive folder. Already-seen files are skipped. |
| **2. Extract** | Each new file is parsed by **LlamaParse** using the LlamaCloud extract agent `More retail Agent`. |
| **3. Sheets** | Rows are appended to tab `mrgrn` of the tracking spreadsheet. |
| **4. Supabase** | `supabase_sink.py --run --source mrgrn` writes the same rows to table `mrgrn_grn`. Any field without a typed column is preserved in `raw_data` (jsonb), so a renamed or new field never fails a run. |
| **5. Run log** | A per-run summary is appended to tab `docs_workflow_logs`. |

## Schedule and entry points

Runs on GitHub Actions via `.github/workflows/scheduler.yml`, cron `0 */3 * * *` (every 3 hours). Can also be triggered manually with **Run workflow**.

The workflow deliberately does **not** call `main()`. `app.py` uses the `schedule` library for standalone local use, and that loop would sit idle burning runner minutes. Actions instead invokes `DocAutomation.run_scheduled_workflow()` directly, then runs the Supabase sink as a separate step.

Recent runs average **~2 minutes**.

## Required secrets

Set under **Settings -> Secrets and variables -> Actions**:

| Secret | Purpose |
|---|---|
| `GOOGLE_CREDENTIALS` | base64 of `credentials.json` (Google OAuth client) |
| `GOOGLE_TOKEN` | base64 of `token.json` (authorized refresh token) |
| `LLAMA_API_KEY` | LlamaCloud key, exported to the app as `LLAMA_CLOUD_API_KEY` |
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | service role key - bypasses RLS for writes |

The workflow base64-decodes the two Google secrets into `credentials.json` / `token.json` at the start of the run and deletes them in an `if: always()` cleanup step.

## Running locally

```bash
pip install -r requirements.txt
# place credentials.json + token.json next to the script
python app.py   # schedule loop; Ctrl-C to stop
```

`supabase_sink.py` is a standalone entry point with its own diagnostics. Run these in order before changing anything in Actions:

```bash
python supabase_sink.py --list-sources          # sources and their tables
python supabase_sink.py --print-schema          # SQL to paste into the Supabase editor
python supabase_sink.py --check                 # config + connectivity + tables exist
python supabase_sink.py --self-test             # insert/read/delete a synthetic row
python supabase_sink.py --run --dry-run --limit 2 --dump-json rows.json
python supabase_sink.py --run --source mrgrn --limit 2
```

`--source` is passed explicitly in the workflow because `.env` is gitignored and the sink would otherwise fall back to its default source.

> `supabase_sink.py` is **copied verbatim** across the GRN/PRN scheduler repos and holds the registry of every source. A fix here has to be copied to the others to stay in sync.

## Files

| File | Role |
|---|---|
| `app.py` | Gmail -> Drive -> extract -> Sheets, class `DocAutomation` |
| `supabase_sink.py` | Drive -> Supabase, shared across scheduler repos |
| `.github/workflows/scheduler.yml` | 3-hourly Actions schedule |
| `requirements.txt` | dependencies |

## Adding a field

The extractor's output reaches Supabase whether or not a typed column exists. To promote a field: extend the `mrgrn` entry in `SOURCES` in `supabase_sink.py`, run `--print-schema`, and apply the `alter table` it prints. Until then, query it as `raw_data->>'your_key'`.
