#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
postprocess_alstyle_yml.py
Исправляет только содержимое <description><![CDATA[ ... ]]></description>:
 - заменяет &quot; и &#34; на обычные двойные кавычки "
 - нормализует ярлыки в <li><strong>…</strong> …</li>, чтобы был ровно один двоеточие
 - подчищает случайные '::' внутри CDATA
Ничего вне <description> не трогает.
"""

import re
import sys
from pathlib import Path

def fix_strong_colons(html: str) -> str:
    """
    В каждом <li><strong>LABEL</strong> VALUE</li>:
    - срезает хвостовые двоеточия/пробелы у LABEL
    - принудительно ставит ровно одно ':' после LABEL
    """
    def _repl(m: re.Match) -> str:
        label = m.group(1)
        # убрать все хвостовые двоеточия/полуширинные/широкие варианты и пробелы
        label = re.sub(r'[:：\s]+$', '', label)
        return f"<li><strong>{label}:</strong>"
    # правим только начало сегмента <li><strong>…</strong>
    return re.sub(r"(?i)<li><strong>([^<]*?)</strong>", _repl, html)

def fix_cdata_block(desc_html: str) -> str:
    """
    Исправления только внутри CDATA описания.
    """
    # 1) HTML-сущности кавычек -> обычные "
    desc_html = desc_html.replace("&quot;", '"').replace("&#34;", '"')

    # 2) Нормализуем ярлыки с двоеточиями
    desc_html = fix_strong_colons(desc_html)

    # 3) Подстраховка: двойные двоеточия -> одно двоеточие
    # (на '://' не влияет, т.к. там нет '::')
    desc_html = desc_html.replace("::", ":")

    return desc_html

def process_file(src: Path, dst: Path) -> None:
    text = src.read_text(encoding="utf-8", errors="ignore")

    # Поочерёдно обрабатываем каждый CDATA-блок в <description>
    pattern = re.compile(
        r"(?is)(<description><!\[CDATA\[)(.*?)(\]\]></description>)"
    )

    def _wrap_repl(m: re.Match) -> str:
        head, inner, tail = m.group(1), m.group(2), m.group(3)
        fixed = fix_cdata_block(inner)
        return f"{head}{fixed}{tail}"

    fixed_text = pattern.sub(_wrap_repl, text)

    # Сохраняем как UTF-8
    dst.write_text(fixed_text, encoding="utf-8")

def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # на случай странной консоли
    except Exception:
        pass

    if len(sys.argv) == 1:
        print("Usage: python postprocess_alstyle_yml.py <in.yml> [out.yml]")
        print("Если out.yml не указан, файл будет перезаписан на месте.")
        sys.exit(1)

    in_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2]) if len(sys.argv) >= 3 else in_path

    if not in_path.exists():
        print(f"ERROR: input file not found: {in_path}")
        sys.exit(2)

    process_file(in_path, out_path)
    if out_path == in_path:
        print(f"OK: updated in-place -> {out_path}")
    else:
        print(f"OK: wrote -> {out_path}")

if __name__ == "__main__":
    main()
