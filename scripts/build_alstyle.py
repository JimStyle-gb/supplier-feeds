#!/usr/bin/env python3
# AlStyle post-process (v12) — защита кодировки, чтобы workflow/commit не падал

import os
import sys
from pathlib import Path


def _read_text_safely(path: str) -> str:
    'Читаем файл максимально устойчиво (cp1251 -> utf-8 -> utf-8 replace), убираем BOM.'
    data = Path(path).read_bytes()

    # BOM (на всякий случай)
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]

    for enc in ("windows-1251", "utf-8"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            pass

    return data.decode("utf-8", errors="replace")


def _write_cp1251_safe(path: str, text: str) -> None:
    'Пишем строго в windows-1251, но не падаем на символах вне кодировки.'
    if not text.endswith("\n"):
        text += "\n"

    data = text.encode("windows-1251", errors="xmlcharrefreplace")
    Path(path).write_bytes(data)


def main() -> int:
    out_file = os.getenv("OUT_FILE", "docs/alstyle.yml").strip() or "docs/alstyle.yml"

    if not Path(out_file).exists():
        print(f"[alstyle post] OUT_FILE not found: {out_file}", file=sys.stderr)
        return 2

    text = _read_text_safely(out_file)

    # ВАЖНО: ничего не правим по структуре/логике, только безопасно перезаписываем в cp1251.
    _write_cp1251_safe(out_file, text)

    print(f"[alstyle post] ok: rewrote as windows-1251 (safe) -> {out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
