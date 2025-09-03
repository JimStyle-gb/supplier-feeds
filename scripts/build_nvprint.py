# -*- coding: utf-8 -*-
"""
NVPrint API → YML (Satu-совместимый)
- Авторизация: заголовок apikey: <ключ> (из Secrets).
- company_id / dogovor_id: можно передать через ENV; если не задать — возьмём первый из /v2/company.
- Продукты: /v2/product?company_id=...&dogovor_id=... (без пагинации).
- Цена: берём KZT из price, без конвертации (как вы и сказали).
- Наличие: сумма stock[].store_amount → quantity/quantity_in_stock/stock_quantity; available/in_stock по сумме > 0.
- Категории: строим дерево от названия раздела; корень = 9400000 "NVPrint".
- Картинки: поддержка списка фото.
- Описание: простая сборка из свойств (без усложнения).
"""

from __future__ import annotations
import os, sys, re, html, hashlib, json
from typing import Any, Dict, List, Optional, Tuple
import requests
from datetime import datetime

API_BASE            = os.getenv("NVPRINT_API_BASE", "https://api.b2b.nvprint.ru").rstrip("/")
API_KEY             = (os.getenv("NVPRINT_API_KEY") or "").strip()

# опционально (если знаем заранее)
NVPRINT_COMPANY_ID  = (os.getenv("NVPRINT_COMPANY_ID") or "").strip()
NVPRINT_DOGOVOR_ID  = (os.getenv("NVPRINT_DOGOVOR_ID") or "").strip()

OUT_FILE            = os.getenv("OUT_FILE", "docs/nvprint.yml")
ENCODING            = (os.getenv("OUTPUT_ENCODING") or "utf-8").lower()          # "utf-8" | "windows-1251"
HTTP_TIMEOUT        = float(os.getenv("HTTP_TIMEOUT", "60"))
SECTION_ID          = (os.getenv("NVPRINT_SECTION_ID") or "").strip()            # необязательно; если задать — фильтруем товары по разделу
MAX_PICTURES        = int(os.getenv("MAX_PICTURES", "10"))                       # сколько <picture> максимально выводить

ROOT_CAT_ID         = 9400000
ROOT_CAT_NAME       = "NVPrint"

UA = {"User-Agent": "Mozilla/5.0 (compatible; NVPrint-Feed/1.0)"}

# -------- helpers --------
def x(s: str) -> str:
    return html.escape(s or "")

def stable_cat_id(text: str, prefix: int = 9420000) -> int:
    h = hashlib.md5((text or "").encode("utf-8", errors="ignore")).hexdigest()[:6]
    return prefix + int(h, 16)

def get_json(session: requests.Session, path: str, params: Dict[str, Any] | None = None) -> Any:
    url = f"{API_BASE.rstrip('/')}/{path.lstrip('/')}"
    headers = {"apikey": API_KEY, **UA}
    r = session.get(url, headers=headers, params=params or {}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def choose_company_and_dogovor(session: requests.Session) -> Tuple[str, str]:
    if NVPRINT_COMPANY_ID and NVPRINT_DOGOVOR_ID:
        return NVPRINT_COMPANY_ID, NVPRINT_DOGOVOR_ID
    data = get_json(session, "/v2/company")
    # ожидаем список компаний; берём первую с договором
    if isinstance(data, dict) and "data" in data:
        items = data["data"]
    else:
        items = data
    for comp in (items or []):
        cid = str(comp.get("company_id") or comp.get("id") or "").strip()
        # договоры могут быть списком в comp.get("dogovor") или полем dogovor_id
        dogovors = comp.get("dogovor") or comp.get("contracts") or []
        if isinstance(dogovors, list) and dogovors:
            did = str(dogovors[0].get("dogovor_id") or dogovors[0].get("id") or "").strip()
            if cid and did:
                return cid, did
        did2 = str(comp.get("dogovor_id") or "").strip()
        if cid and did2:
            return cid, did2
    raise RuntimeError("Не удалось определить company_id / dogovor_id. Передайте через ENV NVPRINT_COMPANY_ID/NVPRINT_DOGOVOR_ID.")

def fetch_products(session: requests.Session, company_id: str, dogovor_id: str, section_id: str | None = None) -> List[Dict[str, Any]]:
    params = {"company_id": company_id, "dogovor_id": dogovor_id}
    if section_id:
        params["section_id"] = section_id
    data = get_json(session, "/v2/product", params)
    if isinstance(data, dict) and "data" in data:
        return data["data"] or []
    return data or []

def pick_price_kzt(p: Dict[str, Any]) -> Optional[float]:
    """Ищем цену в тенге внутри p['price'] без конвертаций."""
    price = p.get("price")
    if price is None:
        return None
    # Варианты структуры:
    # 1) {"price_kzt": 12345, ...}
    # 2) {"currency": "KZT", "price": 12345}
    # 3) {"kzt": 12345} и т.п.
    if isinstance(price, dict):
        for key in ("price_kzt", "kzt", "KZT", "priceKZT", "PriceKZT"):
            if key in price:
                try:
                    return float(price[key])
                except Exception:
                    pass
        # currency=KZT + price/amount
        cur = str(price.get("currency") or "").upper()
        if cur == "KZT":
            for key in ("price", "amount", "value"):
                if key in price:
                    try:
                        return float(price[key])
                    except Exception:
                        pass
        # Популярные альтернативы (если вдруг)
        for key in ("price_kz", "price_kaz"):
            if key in price:
                try:
                    return float(price[key])
                except Exception:
                    pass
    # Иногда цена лежит прямо в p["price"] (число), а p["price"]["currency"]="KZT" отсутствует
    try:
        v = float(price)
        # если это «голое» число — считаем, что это KZT (на ваш страх и риск)
        return v
    except Exception:
        return None

def sum_stock(p: Dict[str, Any]) -> float:
    total = 0.0
    stocks = p.get("stock") or []
    if isinstance(stocks, dict):
        stocks = [stocks]
    for st in stocks:
        try:
            total += float(st.get("store_amount") or st.get("amount") or 0)
        except Exception:
            pass
    return total

def build_category_path(p: Dict[str, Any]) -> List[str]:
    """
    Собираем путь категорий из p['section'].
    Если только одно имя — будет один уровень под корнем 'NVPrint'.
    """
    sec = p.get("section") or {}
    names: List[str] = []
    # Возможные поля: name, section_name, parent_name и т.п.
    nm = sec.get("name") or sec.get("section_name") or ""
    if isinstance(nm, str) and nm.strip():
        names.append(nm.strip())
    # если встречаются родительские пути (например, full_path)
    fp = sec.get("full_path") or sec.get("path") or ""
    if isinstance(fp, str) and ">" in fp:
        # "Принтеры > Картриджи > NVPrint" → добавим недостающие уровни
        parts = [a.strip() for a in fp.split(">") if a.strip()]
        if parts:
            names = parts
    return names

def safe_text(d: Dict[str, Any], key: str) -> str:
    v = d.get(key)
    return str(v) if v is not None else ""

def make_description(p: Dict[str, Any]) -> str:
    prop = p.get("property") or {}
    bits: List[str] = []
    # Коротко и без «воды»
    full_name = prop.get("full_name") or ""
    if full_name:
        bits.append(full_name)
    # основные поля
    for label, key in (
        ("Модель", "model"),
        ("Ресурс", "resurs"),
        ("Цвет", "color"),
        ("Вес", "weight"),
        ("Объем", "volume"),
        ("Штрихкод", "barcode"),
    ):
        val = prop.get(key)
        if isinstance(val, (str, int, float)) and str(val).strip():
            bits.append(f"{label}: {val}")
    # совместимость (если есть markers)
    mrk = prop.get("markers") or []
    if isinstance(mrk, list) and mrk:
        # возьмём первые 6 моделей
        head = ", ".join([str(x) for x in mrk[:6] if x])
        if head:
            bits.append(f"Совместимость: {head}…")
    text = "; ".join(bits)
    # лёгкая нормализация
    text = re.sub(r"\s+", " ", text).strip()
    return text[:3800]  # чтобы не превышать лимиты описания

def build_yml(categories: List[Tuple[int,str,Optional[int]]], offers: List[Tuple[int,Dict[str,Any]]]) -> str:
    # UTF-8 по умолчанию; CP1251 тоже ок для Satu — управляется ENCODING при записи
    out: List[str] = []
    enc_label = "utf-8" if ENCODING.startswith("utf") else "windows-1251"
    out.append(f"<?xml version='1.0' encoding='{enc_label}'?>")
    out.append(f"<yml_catalog date=\"{datetime.now().strftime('%Y-%m-%d %H:%M')}\">")
    out.append("<shop>")
    out.append("<name>nvprint</name>")
    out.append('<currencies><currency id="KZT" rate="1" /></currencies>')
    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{x(ROOT_CAT_NAME)}</category>")
    for cid, name, parent in categories:
        parent = parent if parent else ROOT_CAT_ID
        out.append(f"<category id=\"{cid}\" parentId=\"{parent}\">{x(name)}</category>")
    out.append("</categories>")
    out.append("<offers>")
    for cid, it in offers:
        attrs = ' available="true"' if it.get("available") else ' available="false"'
        attrs += ' in_stock="true"' if it.get("in_stock") else ' in_stock="false"'
        out.append(f"<offer id=\"{x(it['id'])}\"{attrs}>")
        out.append(f"<name>{x(it['name'])}</name>")
        out.append(f"<vendor>{x(it.get('vendor') or 'NV Print')}</vendor>")
        if it.get("vendorCode"): out.append(f"<vendorCode>{x(it['vendorCode'])}</vendorCode>")
        out.append(f"<price>{int(round(float(it['price'])))}</price>")
        out.append("<currencyId>KZT</currencyId>")
        out.append(f"<categoryId>{cid}</categoryId>")
        if it.get("url"): out.append(f"<url>{x(it['url'])}</url>")
        pics = it.get("pictures") or []
        for u in pics[:MAX_PICTURES]:
            out.append(f"<picture>{x(u)}</picture>")
        if it.get("description"):
            out.append(f"<description>{x(it['description'])}</description>")
        # остатки
        qty = int(max(0, int(round(it.get('stock', 0)))))
        out.append(f"<quantity_in_stock>{qty}</quantity_in_stock>")
        out.append(f"<stock_quantity>{qty}</stock_quantity>")
        out.append(f"<quantity>{qty if qty>0 else 1}</quantity>")
        # полезные параметры (если есть)
        params: Dict[str,str] = it.get("params") or {}
        for k, v in params.items():
            if v is None or str(v).strip() == "":
                continue
            out.append(f"<param name=\"{x(k)}\">{x(str(v))}</param>")
        out.append("</offer>")
    out.append("</offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)

def main() -> int:
    if not API_KEY:
        print("ERROR: NVPRINT_API_KEY пуст. Добавьте ключ API в Secrets.", file=sys.stderr)
        return 1

    s = requests.Session()
    s.headers.update(UA)

    company_id, dogovor_id = choose_company_and_dogovor(s)
    products = fetch_products(s, company_id, dogovor_id, SECTION_ID)

    # подготовка данных
    all_paths: List[List[str]] = []
    offers: List[Tuple[int, Dict[str, Any]]] = []

    for p in products:
        pid = str(p.get("product_id") or p.get("id") or "").strip()
        if not pid:
            continue
        prop = p.get("property") or {}
        name = prop.get("full_name") or p.get("name") or ""
        if not isinstance(name, str) or not name.strip():
            continue

        # цена в KZT
        price_kzt = pick_price_kzt(p)
        if price_kzt is None or price_kzt <= 0:
            # если вдруг нет — пропустим товар
            continue

        # остатки
        total_stock = sum_stock(p)
        available = total_stock > 0.0
        in_stock = available

        # картинки
        photos = p.get("photo") or []
        if isinstance(photos, str):
            pictures = [photos]
        elif isinstance(photos, list):
            pictures = [str(u) for u in photos if u]
        else:
            pictures = []

        # ссылка на карточку (если отдают)
        url = p.get("url") or prop.get("url") or ""

        # vendorCode / articul
        vendor_code = prop.get("articul") or p.get("articul") or p.get("code") or ""

        # описание
        descr = make_description(p)

        # категория
        path = build_category_path(p)
        all_paths.append(path)

        # параметры
        params: Dict[str, str] = {}
        # выберем базовые поля из property
        for k_src, k_dst in (
            ("model", "Модель"),
            ("resurs", "Ресурс"),
            ("color", "Цвет"),
            ("weight", "Вес"),
            ("volume", "Объем"),
            ("barcode", "Штрихкод"),
            ("code", "Код NVPrint"),
        ):
            val = prop.get(k_src) if k_src in prop else p.get(k_src)
            if val is not None and str(val).strip():
                params[k_dst] = str(val).strip()

        offers.append((ROOT_CAT_ID, {
            "id": pid,
            "name": str(name).strip(),
            "vendor": "NV Print",
            "vendorCode": str(vendor_code).strip() if vendor_code else "",
            "price": float(price_kzt),
            "url": str(url).strip() if url else "",
            "pictures": pictures,
            "description": descr,
            "stock": total_stock,
            "available": available,
            "in_stock": in_stock,
            "params": params,
        }))

    # строим дерево категорий
    cat_map: Dict[Tuple[str,...], int] = {}
    categories: List[Tuple[int,str,Optional[int]]] = []
    for path in all_paths:
        clean = [p for p in (path or []) if isinstance(p, str) and p.strip()]
        if not clean:
            continue
        parent = ROOT_CAT_ID
        acc: List[str] = []
        for name in clean:
            acc.append(name.strip())
            key = tuple(acc)
            if key in cat_map:
                parent = cat_map[key]; continue
            cid = stable_cat_id(" / ".join(acc))
            cat_map[key] = cid
            categories.append((cid, name.strip(), parent))
            parent = cid

    # Присваиваем categoryId в офферы
    def path_to_id(path: List[str]) -> int:
        key = tuple([p.strip() for p in (path or []) if p and p.strip()])
        return cat_map.get(key, ROOT_CAT_ID)

    offers_final: List[Tuple[int, Dict[str, Any]]] = []
    for i, (cid, it) in enumerate(offers):
        path = all_paths[i] if i < len(all_paths) else []
        offers_final.append((path_to_id(path), it))

    # запись
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(categories, offers_final)
    with open(OUT_FILE, "w", encoding=("utf-8" if ENCODING.startswith("utf") else "cp1251"), errors="ignore") as f:
        f.write(xml)

    print(f"[nvprint] done: {len(offers_final)} offers, {len(categories)} categories -> {OUT_FILE} (encoding={ENCODING})")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e, file=sys.stderr)
        # аварийный пустой файл, чтобы job не падал из-за отсутствия артефакта
        try:
            os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
            with open(OUT_FILE, "w", encoding=("utf-8" if ENCODING.startswith("utf") else "cp1251"), errors="ignore") as f:
                f.write("<?xml version='1.0' encoding='utf-8'?>\n<yml_catalog><shop><name>nvprint</name><currencies><currency id=\"KZT\" rate=\"1\" /></currencies><categories><category id=\"9400000\">NVPrint</category></categories><offers></offers></shop></yml_catalog>")
        except Exception:
            pass
        sys.exit(0)
