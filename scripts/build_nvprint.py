# -*- coding: utf-8 -*-
from __future__ import annotations
import os, sys, re, html, hashlib, json
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import requests

API_BASE            = os.getenv("NVPRINT_API_BASE", "https://api.b2b.nvprint.ru").rstrip("/")
API_KEY             = (os.getenv("NVPRINT_API_KEY") or "").strip()

NVPRINT_COMPANY_ID  = (os.getenv("NVPRINT_COMPANY_ID") or "").strip()
NVPRINT_DOGOVOR_ID  = (os.getenv("NVPRINT_DOGOVOR_ID") or "").strip()

OUT_FILE            = os.getenv("OUT_FILE", "docs/nvprint.yml")
ENCODING            = (os.getenv("OUTPUT_ENCODING") or "utf-8").lower()
HTTP_TIMEOUT        = float(os.getenv("HTTP_TIMEOUT", "60"))
SECTION_ID          = (os.getenv("NVPRINT_SECTION_ID") or "").strip()
MAX_PICTURES        = int(os.getenv("MAX_PICTURES", "10"))
DEBUG_DUMP          = True  # сохраняем служебный дамп /v2/company

ROOT_CAT_ID         = 9400000
ROOT_CAT_NAME       = "NVPrint"

UA = {"User-Agent": "Mozilla/5.0 (compatible; NVPrint-Feed/1.2)"}

def x(s: str) -> str: return html.escape(s or "")

def stable_cat_id(text: str, prefix: int = 9420000) -> int:
    h = hashlib.md5((text or "").encode("utf-8", errors="ignore")).hexdigest()[:6]
    return prefix + int(h, 16)

def get_json(session: requests.Session, path: str, params: Dict[str, Any] | None = None) -> Any:
    url = f"{API_BASE.rstrip('/')}/{path.lstrip('/')}"
    headers = {"apikey": API_KEY, **UA}
    r = session.get(url, headers=headers, params=params or {}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        raise RuntimeError(f"NVPrint API returned non-JSON at {path}: {r.text[:200]}")

def _dump_company_payload(payload: Any):
    try:
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with open("docs/nvprint_company.json", "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _iter_company_items(payload: Any):
    """Итерируем любые возможные места, где могут лежать компании/договоры."""
    if isinstance(payload, list):
        for it in payload:
            if isinstance(it, dict):
                yield it
        return
    if isinstance(payload, dict):
        # прямые ключи
        if any(k in payload for k in ("company_id", "dogovor_id", "id", "contracts", "dogovor")):
            yield payload
        # типовые обёртки
        for key in ("data", "companies", "items", "result", "rows"):
            v = payload.get(key)
            if isinstance(v, list):
                for it in v:
                    if isinstance(it, dict):
                        yield it
            elif isinstance(v, dict):
                # внутри может быть companies/items/list/rows
                for kk in ("companies", "items", "list", "rows"):
                    vv = v.get(kk)
                    if isinstance(vv, list):
                        for it in vv:
                            if isinstance(it, dict):
                                yield it
                # или сам объект с id
                if any(k in v for k in ("company_id","dogovor_id","id","contracts","dogovor")):
                    yield v

def choose_company_and_dogovor(session: requests.Session) -> Tuple[str, str]:
    if NVPRINT_COMPANY_ID and NVPRINT_DOGOVOR_ID:
        return NVPRINT_COMPANY_ID, NVPRINT_DOGOVOR_ID

    payload = get_json(session, "/v2/company")
    if DEBUG_DUMP:
        _dump_company_payload(payload)

    for comp in _iter_company_items(payload):
        # company id
        cid = str(comp.get("company_id") or comp.get("id") or "").strip()

        # договоры в массиве
        dogovors = comp.get("dogovor") or comp.get("contracts") or []
        if isinstance(dogovors, list) and dogovors:
            did = str(dogovors[0].get("dogovor_id") or dogovors[0].get("id") or "").strip()
            if cid and did:
                return cid, did

        # договор одним полем
        did2 = str(comp.get("dogovor_id") or comp.get("contract_id") or "").strip()
        if cid and did2:
            return cid, did2

    raise RuntimeError(
        "Cannot resolve company_id/dogovor_id from /v2/company. "
        "Либо ключ API не даёт доступа, либо ответ в нестандартном формате. "
        "См. дамп: docs/nvprint_company.json. "
        "Либо задайте NVPRINT_COMPANY_ID и NVPRINT_DOGOVOR_ID в Secrets."
    )

def fetch_products(session: requests.Session, company_id: str, dogovor_id: str, section_id: str | None = None) -> List[Dict[str, Any]]:
    params = {"company_id": company_id, "dogovor_id": dogovor_id}
    if section_id:
        params["section_id"] = section_id
    data = get_json(session, "/v2/product", params)
    # допускаем, что data может быть dict с ключом data/items, или сразу list
    items = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        for k in ("data","items","result","rows"):
            v = data.get(k)
            if isinstance(v, list):
                items = v; break
        if not items:
            items = [data]  # fallback
    return [it for it in items if isinstance(it, dict)]

def pick_price_kzt(p: Dict[str, Any]) -> Optional[float]:
    price = p.get("price")
    if price is None:
        return None
    if isinstance(price, dict):
        for key in ("price_kzt","kzt","KZT","priceKZT","PriceKZT","price_kz","price_kaz"):
            if key in price:
                try: return float(price[key])
                except: pass
        cur = str(price.get("currency") or "").upper()
        if cur == "KZT":
            for key in ("price","amount","value"):
                if key in price:
                    try: return float(price[key])
                    except: pass
    try:
        return float(price)  # голое число — считаем KZT
    except: return None

def sum_stock(p: Dict[str, Any]) -> float:
    total = 0.0
    stocks = p.get("stock") or []
    if isinstance(stocks, dict): stocks = [stocks]
    for st in stocks:
        if not isinstance(st, dict): continue
        try:
            total += float(st.get("store_amount") or st.get("amount") or 0)
        except: pass
    return total

def build_category_path(p: Dict[str, Any]) -> List[str]:
    sec = p.get("section") or {}
    fp = sec.get("full_path") or sec.get("path") or ""
    if isinstance(fp, str) and ">" in fp:
        parts = [a.strip() for a in fp.split(">") if a.strip()]
        if parts: return parts
    nm = sec.get("name") or sec.get("section_name") or ""
    return [nm.strip()] if isinstance(nm, str) and nm.strip() else []

def make_description(p: Dict[str, Any]) -> str:
    prop = p.get("property") or {}
    bits: List[str] = []
    full_name = prop.get("full_name") or ""
    if isinstance(full_name, str) and full_name.strip():
        bits.append(full_name.strip())
    for label, key in (("Модель","model"),("Ресурс","resurs"),("Цвет","color"),
                       ("Вес","weight"),("Объем","volume"),("Штрихкод","barcode")):
        v = prop.get(key)
        if v is not None and str(v).strip():
            bits.append(f"{label}: {v}")
    mrk = prop.get("markers") or []
    if isinstance(mrk, list) and mrk:
        head = ", ".join([str(x) for x in mrk[:6] if x])
        if head: bits.append(f"Совместимость: {head}…")
    text = re.sub(r"\s+", " ", "; ".join(bits)).strip()
    return text[:3800]

def build_yml(categories, offers) -> str:
    enc_label = "utf-8" if ENCODING.startswith("utf") else "windows-1251"
    out: List[str] = []
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
        attrs = f' available="{"true" if it.get("available") else "false"}"'
        attrs += f' in_stock="{"true" if it.get("in_stock") else "false"}"'
        out.append(f"<offer id=\"{x(it['id'])}\"{attrs}>")
        out.append(f"<name>{x(it['name'])}</name>")
        out.append(f"<vendor>{x(it.get('vendor') or 'NV Print')}</vendor>")
        if it.get("vendorCode"): out.append(f"<vendorCode>{x(it['vendorCode'])}</vendorCode>")
        out.append(f"<price>{int(round(float(it['price'])))}</price>")
        out.append("<currencyId>KZT</currencyId>")
        out.append(f"<categoryId>{cid}</categoryId>")
        if it.get("url"): out.append(f"<url>{x(it['url'])}</url>")
        for u in (it.get("pictures") or [])[:MAX_PICTURES]:
            out.append(f"<picture>{x(u)}</picture>")
        if it.get("description"):
            out.append(f"<description>{x(it['description'])}</description>")
        qty = max(0, int(round(it.get("stock", 0))))
        out.append(f"<quantity_in_stock>{qty}</quantity_in_stock>")
        out.append(f"<stock_quantity>{qty}</stock_quantity>")
        out.append(f"<quantity>{qty if qty>0 else 1}</quantity>")
        for k, v in (it.get("params") or {}).items():
            vv = "" if v is None else str(v).strip()
            if vv:
                out.append(f"<param name=\"{x(k)}\">{x(vv)}</param>")
        out.append("</offer>")
    out.append("</offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)

def main() -> int:
    if not API_KEY:
        print("ERROR: NVPRINT_API_KEY is empty. Put your API key into repo Secrets.", file=sys.stderr)
        return 1

    s = requests.Session()
    s.headers.update(UA)

    company_id, dogovor_id = choose_company_and_dogovor(s)
    products = fetch_products(s, company_id, dogovor_id, SECTION_ID)

    all_paths: List[List[str]] = []
    offers: List[Tuple[int, Dict[str, Any]]] = []

    for p in products:
        pid = str(p.get("product_id") or p.get("id") or "").strip()
        if not pid:
            continue
        prop = p.get("property") or {}
        name = (prop.get("full_name") or p.get("name") or "").strip()
        if not name:
            continue
        price_kzt = pick_price_kzt(p)
        if price_kzt is None or price_kzt <= 0:
            continue
        total_stock = sum_stock(p)
        available = total_stock > 0
        in_stock = available

        photos = p.get("photo") or []
        pictures = [photos] if isinstance(photos, str) else [str(u) for u in photos if u] if isinstance(photos, list) else []

        url = p.get("url") or prop.get("url") or ""
        vendor_code = prop.get("articul") or p.get("articul") or p.get("code") or ""
        descr = make_description(p)
        path = build_category_path(p)
        all_paths.append(path)

        params: Dict[str, str] = {}
        for k_src, k_dst in (("model","Модель"),("resurs","Ресурс"),("color","Цвет"),
                             ("weight","Вес"),("volume","Объем"),("barcode","Штрихкод"),
                             ("code","Код NVPrint")):
            v = (prop.get(k_src) if k_src in prop else p.get(k_src))
            if v is not None and str(v).strip():
                params[k_dst] = str(v).strip()

        offers.append((ROOT_CAT_ID, {
            "id": pid, "name": name, "vendor": "NV Print",
            "vendorCode": str(vendor_code).strip() if vendor_code else "",
            "price": float(price_kzt), "url": str(url).strip() if url else "",
            "pictures": pictures, "description": descr,
            "stock": total_stock, "available": available, "in_stock": in_stock,
            "params": params,
        }))

    # дерево категорий
    cat_map: Dict[Tuple[str,...], int] = {}
    categories: List[Tuple[int,str,Optional[int]]] = []
    for path in all_paths:
        clean = [p for p in (path or []) if isinstance(p, str) and p.strip()]
        if not clean: continue
        parent = ROOT_CAT_ID; acc: List[str] = []
        for name in clean:
            acc.append(name.strip()); key = tuple(acc)
            if key in cat_map:
                parent = cat_map[key]; continue
            cid = stable_cat_id(" / ".join(acc))
            cat_map[key] = cid
            categories.append((cid, name.strip(), parent))
            parent = cid

    def path_to_id(path: List[str]) -> int:
        key = tuple([p.strip() for p in (path or []) if p and p.strip()])
        return cat_map.get(key, ROOT_CAT_ID)

    offers_final: List[Tuple[int, Dict[str, Any]]] = []
    for i, (cid, it) in enumerate(offers):
        path = all_paths[i] if i < len(all_paths) else []
        offers_final.append((path_to_id(path), it))

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
        try:
            os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
            with open(OUT_FILE, "w", encoding=("utf-8" if ENCODING.startswith("utf") else "cp1251"), errors="ignore") as f:
                f.write("<?xml version='1.0' encoding='utf-8'?>\n<yml_catalog><shop><name>nvprint</name><currencies><currency id=\"KZT\" rate=\"1\" /></currencies><categories><category id=\"9400000\">NVPrint</category></categories><offers></offers></shop></yml_catalog>")
        except Exception:
            pass
        sys.exit(0)
