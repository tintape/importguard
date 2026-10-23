# 90-second demo video script

Screen recording of the running app with the demo data (`python -m importguard.sampledata`), starting from an empty
database. Voice-over in plain English; timings are approximate.

| Time | Screen | Voice-over |
|---|---|---|
| 0:00–0:12 | Dashboard, empty state | "A shop owner gets daily sales exports from the till, the web shop and a marketplace. The files overlap and get re-sent. Merge them by hand and the same revenue is counted twice. ImportGuard makes that merge safe." |
| 0:12–0:27 | Imports: choose *In-store POS*, upload `01_pos_2026-08-31_valid.csv`; job page goes Queued → Succeeded | "I upload today's export and pick its source. A separate worker validates the whole file first. One thousand new sales, nothing skipped." |
| 0:27–0:40 | Upload `02_…(copy).csv`, then `03_…_overlapping.csv` | "Now the same file again, under another name: zero new sales, a thousand duplicates skipped, and it tells me it is byte-identical to import number one. The next day's export overlaps the previous one: 700 new, 300 recognised as the same sales, even though they're written with different time zones and spacing." |
| 0:40–0:58 | Upload `04_…_conflicting_amount.csv`; job page with *Conflicts with existing sales* and the row table | "Here a known sale comes back with a different price. That's a conflict. ImportGuard never overwrites silently, and in strict mode the whole file is rejected, so the 40 genuinely new sales in it are not imported either. I see the row, the field, the old and the new value." |
| 0:58–1:10 | Upload `05_…_invalid_values.csv`; scroll the problems list; click *Download error report* | "Bad dates, commas as decimal separators, negative prices: every problem with its row number, and a CSV report I can send back to whoever produced the file." |
| 1:10–1:23 | Dashboard with per-currency cards and the daily chart; Sales page filtered by store, *Export CSV* | "The report only ever counts fully accepted files. Revenue, sales and average sale per currency, never added across currencies. Filter by date, source or store, and export exactly what I see." |
| 1:23–1:30 | Import page showing "Completed on attempt 2 after an interrupted attempt" | "And if the worker crashes mid-import, nothing half-done shows up: it restarts and finishes, with no duplicates. Synthetic data, real checks; the code and tests are on GitHub." |

Recording notes: 1440 × 900 window, light theme, browser zoom 100%. For the last shot, start a large import
(`10_marketplace_large_100k.csv`), run `kill -9` on the worker while the job says *Processing*, and start the worker
again.
