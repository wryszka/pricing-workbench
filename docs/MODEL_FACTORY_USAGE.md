# Model Factory — usage guide

The Model Factory tab (`/models`) has two sides: **Demo** and **Real**. They look identical by design.
Same UX, same 4-step flow, same narrative from Claude — but Demo generates metrics virtually (no
training) and Real fits every variant against the live Modelling Mart.

Candidates trained through the Real tab stay **outside the main pricing flow** — they register as
`factory_freq_glm_*` UC models alongside the four production champions and never claim the
`champion` alias. No factory candidate is served or deployed.

## When to use each tab

| Use case | Tab |
|---|---|
| Demo narrative, walkthroughs, fast iteration | **Demo** |
| "Show me it's real" — regulator in the room, proof the platform fits models | **Real** |
| Anything going near production | Neither — use Model Development → Train/Compare/Promote for that |

## Running the Real tab end-to-end

### Step 1 — Propose plan

1. Open the app at `/models`
2. Click the **Real** tab (emerald "fits models" badge)
3. Family is pre-set to Frequency (GLM). Other families come later.
4. Click **Propose plan** — Claude narrates the rationale live via Foundation Model API (~2-3 paragraphs)
5. A 15-variant table appears — mix of feature-subset, interaction, and banding variants across 4 GLM families (Poisson, Quasi-Poisson, Neg Binomial, Tweedie)
6. Review the plan; modifications aren't in MVP — approve as-is

### Step 2 — Train

1. Click **Approve plan & train**
2. Progress bar shows variants trained as they complete
3. Typical wall clock: 3-5 min for 15 variants on serverless (first job run adds ~60s for cold-start)
4. Each variant is:
   - Fitted on the Modelling Mart (50k policies, 80/20 split, seeded)
   - 5-fold CV Gini computed
   - Logged to MLflow experiment `/Users/<you>/pricing_workbench_factory`
   - Registered in UC as `factory_freq_glm_{variant_id}` (A01, B01, C01, …)

### Step 3 — Review

Three tiers:

**Leaderboard** — all 15 variants sorted by Gini (click any column header to re-sort by AIC, BIC, deviance explained, MAE). Top 5 rows highlighted emerald.

**Shortlist** — top 5 auto-picked. Each row shows:
- Headline Gini + AIC/BIC
- Real 5-fold CV Gini mean ± σ, with stability flag (stable if σ < 0.015)
- Sign-check badges (expected direction on flood_zone, credit_score, is_coastal)
- Full config (features, interactions, banding, GLM family)

(The Portfolio what-if tier is hidden in Real mode — portfolio scoring lands in a later iteration.)

**Agent chat (right panel)** — Claude Sonnet 4.6 grounded in the run's actual leaderboard + shortlist + plan narrative. Useful questions:
- "Which variants look most stable?"
- "Does adding interactions help?"
- "Compare A01 and B03"
- "Any red flags in the shortlist?"

### Step 4 — Selective packaging

1. Tick the checkbox on any shortlist variants you want to package
2. Click the **Generate real pack** button (emerald, top-right of each row) — or the Generate pack(s) bar at the bottom for a bulk run
3. The pack generation job `v1 — Generate governance pack` is triggered **for real** — uses the factory candidate's registered UC model + its MLflow artefacts to build a full 8-section PDF + all sidecars (model_card.md, metrics.json, importance.parquet, fairness.md, lineage.json, approvals.json, sidecars delta row for the agent)
4. Pack lands at `/Volumes/{catalog}/{schema}/governance_packs/factory_freq_glm_<variant_id>_<ts>.pdf`
5. Indexed in `governance_packs_index` — discoverable from the Model Governance tab **By Model** entry point if you scroll to factory families

## What's deliberately out of scope

- **No promotion to deployment.** Factory candidates don't flip the `champion` alias, don't show on the Model Deployment tab, and don't appear on the Promote tab. If a candidate looks good, the next step is to re-train it through the production pipeline (`production_training.yml`) and promote that version.
- **No portfolio what-if.** The simulated portfolio views on the Demo tab are synthetic; adding real portfolio scoring for factory candidates needs the Compare & Test scoring path finished (separate work item).
- **Only freq_glm.** Severity, demand, and fraud families show "soon" chips on the selector in the Real tab.

## Under the hood — where things land

| Artefact | Location |
|---|---|
| Factory run metadata | `{catalog}.{schema}.factory_runs` (rows with `run_id` starting `REAL-FACTORY-...`) |
| Per-variant real metrics | `{catalog}.{schema}.factory_variants` (real metrics, not synthetic) |
| MLflow runs per variant | `/Users/<you>/pricing_workbench_factory` experiment |
| Registered UC models | `{catalog}.{schema}.factory_freq_glm_<variant_id>` |
| Pack PDFs (if generated) | `/Volumes/{catalog}/{schema}/governance_packs/` |
| Pack sidecars | `/Volumes/{catalog}/{schema}/governance_packs/sidecars/<pack_id>/` + `{catalog}.{schema}.governance_pack_sidecars` |
| Audit log | `{catalog}.{schema}.audit_log` — `factory_plan_approved_real`, `factory_variant_pack_requested`, `governance_pack_generated` |

## Cleaning up factory models

Since factory candidates accumulate in UC, clean them out periodically:

```python
# From a Databricks notebook
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
models = w.registered_models.list(
    catalog_name="lr_serverless_aws_us_catalog",
    schema_name="pricing_upt",
)
for m in models:
    if m.name.startswith("factory_freq_glm_"):
        # Delete all versions first, then the model
        for v in w.model_versions.list(full_name=f"{m.full_name}"):
            w.model_versions.delete(full_name=m.full_name, version=int(v.version))
        w.registered_models.delete(full_name=m.full_name)
```

Or via SQL:

```sql
-- List them
SELECT table_name
FROM lr_serverless_aws_us_catalog.information_schema.registered_models
WHERE schema_name = 'pricing_upt' AND table_name LIKE 'factory_freq_glm_%';

-- Drop one at a time (UC enforces version deletion first — use the SDK above for bulk)
DROP MODEL IF EXISTS lr_serverless_aws_us_catalog.pricing_upt.factory_freq_glm_A01;
```

## Troubleshooting

**"Job 'v1 — Factory training (real)' not found"** — run `databricks bundle deploy --profile DEFAULT` to push the bundle, then grant the app SP `CAN_MANAGE_RUN` on the job (the bundle's default perms don't include the app SP).

**Training stalls** — check the job run page linked in the `run_page_url` field of the approve response. Serverless cold-start can take 60-90s before any variant metrics appear.

**Pack generation fails for a factory variant** — check `{catalog}.{schema}.audit_log` for `governance_pack_requested` events; the pack job's failure detail shows up there. The most common cause is the variant's UC model wasn't registered (training failed silently on that variant). Re-run the whole factory run.

**"Insufficient features"** — some variants may skip if the Modelling Mart drops a column. This is expected; the training log shows `{variant_id}: skipped — insufficient features`.
