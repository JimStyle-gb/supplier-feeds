#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# build_alstyle.py (v14, compact)
# Назначение: скачать фид Al‑Style, отфильтровать/почистить офферы, префиксовать vendorCode,
# перенести <available> в атрибут, перенести <purchase_price> -> <price>, отсортировать теги и сохранить cp1251.

from __future__ import annotations
import sys, time, re, pathlib, xml.etree.ElementTree as ET
import requests
from requests.auth import HTTPBasicAuth

# --- Конфиг ---
SUPPLIER_URL = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
USERNAME = "info@complex-solutions.kz"      # логин (BasicAuth)
PASSWORD = "Aa123456"                        # пароль
ALLOWED_CATEGORY_IDS_CSV = ("3540,3541,3542,3543,3544,3545,3566,3567,3569,3570,3580,3688,3708,3721,3722,"
                            "4889,4890,4895,5017,5075,5649,5710,5711,5712,5713,21279,21281,21291,21356,"
                            "21367,21368,21369,21370,21371,21372,21451,21498,21500,21501,21572,21573,21574,"
                            "21575,21576,21578,21580,21581,21583,21584,21585,21586,21588,21591,21640,21664,"
                            "21665,21666,21698")
ALLOWED_CATEGORY_IDS = {x.strip() for x in ALLOWED_CATEGORY_IDS_CSV.split(",") if x.strip()}
PARAMS_TO_DROP = {  # имена <param>, которые удаляем (сравнение по нормализованному имени)
    "артикул","благотворительность","код тн вэд","код товара kaspi","новинка","снижена цена",
    "штрихкод","штрих-код","назначение","объем","объём"
}
STRIP_OFFER_TAGS = {"url","quantity","quantity_in_stock"}  # служебные теги для удаления (кроме цены)
OFFER_TAG_ORDER = ["categoryId","vendorCode","name","price","picture","vendor","currencyId","description","param"]
OUT_FILE = pathlib.Path("docs/alstyle.yml"); OUTPUT_ENCODING = "windows-1251"
TIMEOUT_S, RETRY, SLEEP_BETWEEN_RETRY = 45, 2, 2
HEADERS = {"User-Agent":"AlStyleFeedBot/1.0 (+github-actions)"}

# --- Утилиты ---
def _ensure_dirs(p: pathlib.Path):  # создать каталог вывода
    p.parent.mkdir(parents=True, exist_ok=True)

def _offers(root: ET.Element):  # получить узел <offers>
    shop = root.find("./shop");  return None if shop is None else shop.find("offers")

def _fetch(url: str):  # скачать XML (без auth -> с auth) с ретраями
    for _ in range(RETRY+1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S)
            if r.status_code==200 and r.content: return r.content
        except requests.RequestException: pass
        time.sleep(SLEEP_BETWEEN_RETRY)
    auth = HTTPBasicAuth(USERNAME, PASSWORD)
    for _ in range(RETRY+1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S, auth=auth)
            if r.status_code==200 and r.content: return r.content
        except requests.RequestException: pass
        time.sleep(SLEEP_BETWEEN_RETRY)
    return None

def _write_windows_1251(path: pathlib.Path, xml_unicode: str):  # сохранить cp1251 с заголовком
    decl = '<?xml version="1.0" encoding="windows-1251"?>\n'
    data = (decl + xml_unicode).encode(OUTPUT_ENCODING, errors="xmlcharrefreplace")
    path.write_bytes(data)

def _norm_param_name(name: str):  # нормализовать имя параметра
    s = (name or "").replace("\u00A0"," ").strip().lower()
    s = re.sub(r"[,.;:]+$","",s);  s = re.sub(r"\s+"," ",s);  return s

# --- Шаги ---
def step_filter_by_category(root: ET.Element):  # оставить офферы с нужными categoryId
    offers = _offers(root);  total=kept=dropped=0
    if offers is None: return (0,0,0)
    for off in list(offers):
        total+=1; cat_el = off.find("categoryId"); cat = (cat_el.text or "").strip() if cat_el is not None else ""
        if cat.isdigit(): cat = str(int(cat))  # "021" -> "21"
        (kept:=kept+1) if cat in ALLOWED_CATEGORY_IDS else (offers.remove(off), (dropped:=dropped+1))
    return (total,kept,dropped)

_TRUE = {"true","1","yes","y","да","есть","в наличии","наличие","есть в наличии"}
_FALSE = {"false","0","no","n","нет","отсутствует","нет в наличии","под заказ","ожидается"}
def _to_bool_text(v: str):  # привести к "true"/"false"
    s=(v or "").strip().lower().replace(":"," ").replace("\u00A0"," ")
    if s in _TRUE or "true" in s or "да" in s: return "true"
    if s in _FALSE or "false" in s or "нет" in s or "под заказ" in s: return "false"
    return "false"

def step_migrate_available(root: ET.Element):  # перенести <available> в offer@available и удалить тег
    offers = _offers(root); seen=set_cnt=overr=removed=0
    if offers is None: return (0,0,0,0)
    for off in list(offers):
        seen+=1; av = off.find("available")
        if av is not None:
            val = _to_bool_text(av.text or "")
            if off.get("available") and off.get("available")!=val: overr+=1
            elif not off.get("available"): set_cnt+=1
            off.set("available", val); off.remove(av); removed+=1
    return (seen,set_cnt,overr,removed)

def step_prune_shop_prefix(root: ET.Element):  # удалить всё в <shop> до <offers>
    shop = root.find("./shop");  removed=0
    if shop is None: return 0
    offers = shop.find("offers");  
    if offers is None: return 0
    for child in list(shop):
        if child is offers: break
        shop.remove(child); removed+=1
    return removed

def step_strip_offer_fields(root: ET.Element):  # перенести purchase_price->price и удалить служебные теги
    offers = _offers(root);  prices_set=pp_removed=other_removed=0
    if offers is None: return (0,0,0)
    for off in list(offers):
        pp = off.findall("purchase_price")
        if pp:
            new_text = ""
            for p in pp:
                t=(p.text or "").strip()
                if t: new_text=t  # возьмём последнее непустое
            for old in off.findall("price"): off.remove(old)  # убрать старые цены
            if new_text:
                np = ET.Element("price"); np.text = new_text
                for k,v in pp[-1].attrib.items(): np.set(k,v)  # перенести атрибуты последнего pp
                off.append(np); prices_set+=1
            for p in pp: off.remove(p); pp_removed+=1
        for el in list(off):  # убрать url/quantity/quantity_in_stock
            if el.tag in STRIP_OFFER_TAGS: off.remove(el); other_removed+=1
    return (prices_set,pp_removed,other_removed)

def step_strip_params_by_name(root: ET.Element):  # удалить <param> из стоп-листа
    offers = _offers(root);  removed=0
    if offers is None: return 0
    bad = {_norm_param_name(x) for x in PARAMS_TO_DROP}
    for off in list(offers):
        for p in list(off.findall("param")):
            if _norm_param_name(p.attrib.get("name") or "") in bad:
                off.remove(p); removed+=1
    return removed

def step_prefix_vendorcode_and_sync_id(root: ET.Element, prefix="AS"):  # добавить префикс AS и синхронизировать offer@id
    offers = _offers(root);  upd=chg=0
    if offers is None: return (0,0)
    for off in list(offers):
        vc = off.find("vendorCode")
        if vc is None: continue
        old=(vc.text or "").strip(); new=f"{prefix}{old}"
        if new!=old: vc.text=new; chg+=1
        off.set("id", new); upd+=1
    return (upd,chg)

def step_reorder_offer_children(root: ET.Element):  # отсортировать теги внутри каждого <offer>
    offers = _offers(root);  processed=0
    if offers is None: return 0
    order = OFFER_TAG_ORDER
    for off in list(offers):
        childs=list(off)
        if not childs: continue
        buckets={}
        for c in childs: buckets.setdefault(c.tag, []).append(c)
        new=[]
        for t in order:
            if t in buckets: new.extend(buckets.pop(t))
        for c in childs:
            lst=buckets.get(c.tag)
            if lst: new.append(lst.pop(0));  not lst and buckets.pop(c.tag, None)
        for c in list(off): off.remove(c)
        off.extend(new); processed+=1
    return processed

# --- main ---
def main():
    print(">> Download feed...")
    raw=_fetch(SUPPLIER_URL)
    if not raw: print("!! Download failed", file=sys.stderr); return 2
    try: root=ET.fromstring(raw)
    except ET.ParseError as e: print(f"!! XML parse error: {e}", file=sys.stderr); return 3
    if root.tag.lower()!="yml_catalog": print("!! Root is not <yml_catalog>", file=sys.stderr); return 4

    total,kept,dropped = step_filter_by_category(root); print(f">> Filter: total={total}, kept={kept}, dropped={dropped}")
    seen,set_cnt,overr,rem = step_migrate_available(root); print(f">> Available: seen={seen}, set={set_cnt}, override={overr}, removed={rem}")
    pruned = step_prune_shop_prefix(root); print(f">> Prune shop prefix: removed_nodes={pruned}")
    pset,pprem,other = step_strip_offer_fields(root); print(f">> Offer fields: price_set={pset}, purchase_price_removed={pprem}, other_removed={other}")
    prem = step_strip_params_by_name(root); print(f">> Params removed: {prem}")
    upd,chg = step_prefix_vendorcode_and_sync_id(root, prefix="AS"); print(f">> VendorCode/id: offers={upd}, changed={chg}")
    reord = step_reorder_offer_children(root); print(f">> Reordered offers: {reord}")

    xmlu = ET.tostring(root, encoding="unicode")
    _ensure_dirs(OUT_FILE); _write_windows_1251(OUT_FILE, xmlu)
    print(f">> Written: {OUT_FILE}"); return 0

if __name__=="__main__": raise SystemExit(main())
