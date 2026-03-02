name: Build_AkCent

on:
  workflow_dispatch:
  schedule:
    - cron: '0 21 * * *'

permissions:
  contents: write
jobs:
  build_akcent:
    runs-on: ubuntu-latest
    concurrency:
      group: build_akcent
      cancel-in-progress: true
    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          if [ -f requirements.txt ]; then
            pip install -r requirements.txt
          else
            pip install requests beautifulsoup4 lxml pyyaml python-dateutil openpyxl
          fi

      - name: Configure git identity (GitHub Actions bot)
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

      - name: Smoke test (py_compile)
        run: |
          python -m py_compile scripts/cs/core.py scripts/build_akcent.py
      - name: Build feed (akcent)
        run: |
          python scripts/build_akcent.py


      - name: Diagnostics (git status / diff / sizes)
        run: |
          git status --porcelain
          git diff --stat || true
          ls -la docs docs/raw || true
          wc -c docs/akcent.yml docs/raw/akcent.yml 2>/dev/null || true
      - name: Commit & push docs/akcent.yml (with heartbeat)
        run: |
          # Всегда фиксируем время успешного прогона, даже если фид не изменился
          TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
          mkdir -p docs/raw
          echo "$TS" > docs/raw/akcent_last_run.txt

          git add docs/akcent.yml docs/raw/akcent.yml docs/raw/akcent_last_run.txt

          if git diff --cached --quiet; then
            echo "No changes detected (including heartbeat)."
            exit 0
          fi

          # Если изменился только heartbeat — это тоже валидный признак успешного прогона
          if [ "$(git diff --cached --name-only | wc -l)" -eq 1 ] && git diff --cached --name-only | grep -qx "docs/raw/akcent_last_run.txt"; then
            git commit -m "build: akcent heartbeat ($TS)"
          else
            git commit -m "build: akcent feed"
          fi

          git pull --rebase --autostash
          git push
