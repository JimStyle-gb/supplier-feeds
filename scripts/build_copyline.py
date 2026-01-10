name: Build_CopyLine

on:
  workflow_dispatch:
  push:
    paths:
      - "scripts/build_copyline.py"
      - "scripts/cs/**"
      - ".github/workflows/build_copyline.yml"
  schedule:
    # 03:00 Алматы = 22:00 UTC (предыдущий день). Запускаем ежедневно и режем по дню месяца в Алматы в gate шаге.
    - cron: "0 22 * * *"

concurrency:
  group: feeds_${{ github.ref_name }}
  cancel-in-progress: false

jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: write

    env:
      PUBLIC_VENDOR: "CopyLine"
      OUTPUT_ENCODING: "utf-8"
      PYTHONUTF8: "1"

      # расписание (для gate)
      SCHEDULE_DOM: "1,10,20"
      SCHEDULE_HOUR_ALMATY: "3"

      # источник прайса
      XLSX_URL: "https://copyline.kz/files/price-CLA.xlsx"

      # подсказка для адаптера (если он умеет подкачивать сайт)
      SEED_URLS: "https://copyline.kz/,https://copyline.kz/goods.html"
      NO_CRAWL: "0"

      PIP_PACKAGES: "requests beautifulsoup4 lxml openpyxl"

    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Gate (schedule only)
        id: gate
        shell: bash
        run: |
          python - <<'PY'
          import os
          from datetime import datetime, timedelta

          dom = (os.getenv("SCHEDULE_DOM", "*") or "*").strip()
          hour = int(os.getenv("SCHEDULE_HOUR_ALMATY", "0") or "0")
          event = (os.getenv("GITHUB_EVENT_NAME", "") or "").strip()

          now_utc = datetime.utcnow()
          now_alm = now_utc + timedelta(hours=5)
          almaty_now = now_alm.strftime("%Y-%m-%d %H:%M:%S")

          if dom == "*":
              allowed_set = None
              allowed = "*"
              day_ok = True
          else:
              allowed_set = {int(x.strip()) for x in dom.split(",") if x.strip().isdigit()}
              allowed = ",".join(str(x) for x in sorted(allowed_set)) if allowed_set else "(empty)"
              day_ok = (now_alm.day in allowed_set) if allowed_set else False

          # Для schedule ограничение делаем только по дню месяца в Алматы.
          # Час задается cron, а GitHub может задержать запуск, поэтому hour_ok не проверяем.
          if event == "schedule":
              should = "yes" if day_ok else "no"
          else:
              should = "yes"

          print(f"::notice::Event={event}; Almaty now: {almaty_now}; allowed_dom={allowed}; hour={hour}; day_ok={day_ok}; should_run={should}")

          # output для if:
          with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
              f.write(f"run={should}\n")

          # фиксируем build_time для FEED_META:
          with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as f:
              f.write(f"CS_FORCE_BUILD_TIME_ALMATY={almaty_now}\n")
          PY

      - name: Setup Python
        if: ${{ github.event_name != 'schedule' || steps.gate.outputs.run == 'yes' }}
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install deps
        if: ${{ github.event_name != 'schedule' || steps.gate.outputs.run == 'yes' }}
        shell: bash
        run: |
          set -euo pipefail
          python -m pip install --upgrade pip
          pip install $PIP_PACKAGES

      - name: Build feed
        if: ${{ github.event_name != 'schedule' || steps.gate.outputs.run == 'yes' }}
        shell: bash
        run: |
          set -euo pipefail
          python scripts/build_copyline.py

      - name: Commit + push docs/copyline.yml (retry)
        if: ${{ github.event_name != 'pull_request' && (github.event_name != 'schedule' || steps.gate.outputs.run == 'yes') }}
        shell: bash
        run: |
          set -euo pipefail

          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

          git add docs/copyline.yml

          if git diff --cached --quiet; then
            echo "No changes."
            exit 0
          fi

          git commit -m "CopyLine: update feed"

          BRANCH="${{ github.ref_name }}"
          for i in 1 2 3; do
            echo "Push attempt $i/3"
            git fetch origin "$BRANCH"
            git rebase "origin/$BRANCH" || { git rebase --abort || true; exit 1; }
            if git push origin "HEAD:$BRANCH"; then
              exit 0
            fi
            echo "Push rejected, retry..."
            sleep 2
          done
          echo "Push failed after retries"
          exit 1
