name: build_copyline

on:
  schedule:
    # Запускаем редко: 21:00 UTC в 9/19/30/31 (это 03:00 Asia/Almaty на 10/20/1)
    - cron: "0 21 9,19,30,31 * *"
  workflow_dispatch:
    inputs:
      force:
        description: "Запустить сразу, игнорируя тайм-гейт (для ручного запуска)"
        required: false
        default: "true"

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: Decide run flag (schedule vs manual)
        id: runflag
        shell: bash
        run: |
          set -e
          EVENT="${{ github.event_name }}"
          FORCE="${{ inputs.force }}"
          if [ "$EVENT" = "schedule" ]; then
            # Тайм-гейт: строго 03:00 Asia/Almaty и дни 1/10/20
            export TZ=Asia/Almaty
            echo "Now (Almaty): $(date)"
            DAY=$(date +%-d)
            HOUR=$(date +%H)
            if [[ "$DAY" =~ ^(1|10|20)$ ]] && [ "$HOUR" = "03" ]; then
              echo "run=true" >> "$GITHUB_OUTPUT"
              echo "Gate: pass (scheduled)."
            else
              echo "run=false" >> "$GITHUB_OUTPUT"
              echo "Gate: skip (scheduled but not 03:00 on 1/10/20)."
            fi
          else
            # Ручной запуск: уважаем переключатель force (по умолчанию true)
            if [ "${FORCE:-true}" = "true" ]; then
              echo "run=true" >> "$GITHUB_OUTPUT"
              echo "Manual run: force=true => pass."
            else
              echo "run=false" >> "$GITHUB_OUTPUT"
              echo "Manual run: force=false => skip."
            fi
          fi

      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        if: steps.runflag.outputs.run == 'true'
        with:
          python-version: "3.11"

      - name: Install deps
        if: steps.runflag.outputs.run == 'true'
        run: |
          python -V
          pip install --upgrade pip
          pip install requests beautifulsoup4 openpyxl

      - name: Build Copyline YML
        if: steps.runflag.outputs.run == 'true'
        env:
          XLSX_URL: https://copyline.kz/files/price-CLA.xlsx
          KEYWORDS_FILE: docs/copyline_keywords.txt
          OUT_FILE: docs/copyline.yml
          OUTPUT_ENCODING: windows-1251
          HTTP_TIMEOUT: "25"
          REQUEST_DELAY_MS: "120"
          MIN_BYTES: "900"
          MAX_CRAWL_MINUTES: "60"
          MAX_CATEGORY_PAGES: "1200"
          MAX_WORKERS: "6"
          VENDORCODE_PREFIX: "CL"
        run: |
          set -e
          python scripts/build_copyline.py

      - name: Commit & push
        if: steps.runflag.outputs.run == 'true'
        run: |
          set -e
          git config user.name  "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add docs/copyline.yml || true
          git commit -m "chore(copyline): update docs/copyline.yml [skip ci]" || echo "No changes"
          git pull --rebase || true
          git push || true

      - name: Skipped
        if: steps.runflag.outputs.run != 'true'
        run: echo "Skipping build — not scheduled time and no force."
