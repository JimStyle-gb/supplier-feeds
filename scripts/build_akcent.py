# -*- coding: utf-8 -*-
"""
Build Akcent YML/XML (flat <offers>) for Satu.

Особенности:
- Фильтр ключевых слов (режим include) по <name>, список: docs/akcent_keywords.txt
- Кодировки ключей: utf-8-sig, utf-8, utf-16, utf-16-le, utf-16-be, windows-1251
- vendor сохраняем ВСЕ, кроме alstyle/vtt/copyline/akcent
- vendorCode заполняется из артикула/имени/URL, префикс AC
- Цены пересчитываются по правилам, хвост .900
- Характеристики из <param> в описание
- available=true у всех, чистка лишних тегов
- FEED_META на русском
"""

from __future__ import annotations
import os, sys, re, time, random, urllib.parse
from copy import deepcopy
from typing import List, Tuple, Dict, Set, Optional
from xml.etree import ElementTree as ET
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
import requests

# --- SETTINGS ---
SUPPLIER_NAME   = os.getenv("SUPPLIER_NAME", "akcent")
SUPPLIER_URL    = os.getenv("SUPPLIER_URL", "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml")
OUT_FILE_YML    = os.getenv("OUT_FILE", "docs/akcent.yml")
OUT_FILE_XML    = "docs/akcent.xml"
ENC             = os.getenv("OUTPUT_ENCODING", "windows-1251")

TIMEOUT_S       = int(os.getenv("TIMEOUT_S", "30"))
RETRIES         = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF   = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "1500"))

VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AC")
VENDORCODE_CREATE_IF_MISSING = os.getenv("VENDORCODE_CREATE_IF_MISSING", "1").lower() in {"1","true","yes"}

# --- Keywords filter ---
AKCENT_KEYWORDS_PATH  = os.getenv("AKCENT_KEYWORDS_PATH", "docs/akcent_keywords.txt")
AKCENT_KEYWORDS_MODE  = os.getenv("AKCENT_KEYWORDS_MODE", "include").lower()
AKCENT_KEYWORDS_DEBUG = os.getenv("AKCENT_KEYWORDS_DEBUG", "0").lower() in {"1","true","yes"}

def log(msg: str): print(msg, flush=True)

# --- Helpers ---
def _norm_name(s: str) -> str:
    return re.sub(r"\s+"," ", (s or "").replace("\u00A0"," ").lower().replace("ё","е")).strip()

class KeySpec:
    __slots__=("raw","kind","pattern")
    def __init__(self, raw:str, kind:str, pattern): self.raw,self.kind,self.pattern=raw,kind,pattern

def load_keywords(path:str)->List[KeySpec]:
    if not os.path.exists(path): return []
    data=None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path,"r",encoding=enc) as f: txt=f.read()
            data=txt.replace("\ufeff","").replace("\x00",""); break
        except: continue
    if data is None:
        with open(path,"r",encoding="utf-8",errors="ignore") as f: data=f.read().replace("\x00","")
    keys=[]
    for ln in data.splitlines():
        s=ln.strip()
        if not s or s.startswith("#"): continue
        if s.startswith("~="):
            rg=re.compile(r"\b"+re.escape(_norm_name(s[2:]))+r"\b",re.I)
            keys.append(KeySpec(s,"word",rg))
        elif s.startswith("/") and s.endswith("/"):
            try: keys.append(KeySpec(s,"regex",re.compile(s[1:-1],re.I)))
            except: pass
        else:
            keys.append(KeySpec(_norm_name(s),"substr",None))
    return keys

def name_matches(name:str,keys:List[KeySpec])->Tuple[bool,Optional[str]]:
    n=_norm_name(name)
    for ks in keys:
        if ks.kind=="substr" and ks.raw in n: return True,ks.raw
        if ks.kind in {"regex","word"} and ks.pattern.search(name): return True,ks.raw
    return False,None

# --- Vendor handling ---
def _norm_key(s:str)->str:
    return re.sub(r"\s+"," ",(s or "").strip().lower().replace("ё","е"))

SUPPLIER_BLOCKLIST={_norm_key(x) for x in["alstyle","al-style","copyline","akcent","ak-cent","vtt"]}
UNKNOWN_VENDOR_MARKERS=("неизвест","unknown","без бренда","no brand","noname","no-name")

def normalize_brand(raw:str)->str:
    k=_norm_key(raw)
    if not k or k in SUPPLIER_BLOCKLIST: return ""
    return raw.strip()

def ensure_vendor(shop_el:ET.Element)->Tuple[int,Dict[str,int]]:
    offers_el=shop_el.find("offers"); norm=0; dropped={}
    for offer in offers_el.findall("offer"):
        ven=offer.find("vendor")
        txt=(ven.text or "").strip() if ven is not None else ""
        if txt:
            canon=normalize_brand(txt)
            if any(m in txt.lower() for m in UNKNOWN_VENDOR_MARKERS) or not canon:
                if ven is not None: offer.remove(ven); dropped[_norm_key(txt)]=dropped.get(_norm_key(txt),0)+1
            elif canon!=txt: ven.text=canon; norm+=1
    return norm,dropped

# --- Pricing ---
PRICE_RULES=[(101,10000,4.0,3000),(10001,25000,4.0,4000),(25001,50000,4.0,5000),(50001,75000,4.0,7000),
(75001,100000,4.0,10000),(100001,150000,4.0,12000),(150001,200000,4.0,15000),(200001,300000,4.0,20000),
(300001,400000,4.0,25000),(400001,500000,4.0,30000),(500001,750000,4.0,40000),(750001,1000000,4.0,50000),
(1000001,1500000,4.0,70000),(1500001,2000000,4.0,90000),(2000001,100000000,4.0,100000)]

def parse_price(raw:str):
    if not raw: return None
    s=re.sub(r"[^\d.,]","",raw).replace(",",".")
    try: return float(s)
    except: return None

def get_dealer_price(offer:ET.Element):
    vals=[]
    for tag in["purchase_price","wholesale_price","opt_price","b2b_price","price"]:
        el=offer.find(tag)
        if el is not None: v=parse_price(el.text or "")
        else: v=None
        if v: vals.append(v)
    return min(vals) if vals else None

def compute_price(dealer:float):
    for lo,hi,pct,add in PRICE_RULES:
        if lo<=dealer<=hi: return int((dealer*(1+pct/100)+add)//1000*1000+900)

# --- Stock ---
def force_available(shop_el:ET.Element)->int:
    offers=shop_el.find("offers"); c=0
    for o in offers.findall("offer"):
        av=o.find("available") or ET.SubElement(o,"available")
        av.text="true"; c+=1
    return c

# --- Main ---
def main():
    data=requests.get(SUPPLIER_URL,timeout=TIMEOUT_S).content
    root=ET.fromstring(data)
    shop_in=root.find("shop"); offers_in=shop_in.find("offers")
    out_root=ET.Element("yml_catalog"); out_root.set("date",time.strftime("%Y-%m-%d %H:%M"))
    out_shop=ET.SubElement(out_root,"shop"); out_offers=ET.SubElement(out_shop,"offers")

    for o in offers_in.findall("offer"):
        mod=deepcopy(o)
        for n in mod.findall("categoryId"): mod.remove(n)
        out_offers.append(mod)

    keys=load_keywords(AKCENT_KEYWORDS_PATH)
    filtered=0
    if keys:
        for off in list(out_offers.findall("offer")):
            nm=(off.find("name").text or "")
            hit,_=name_matches(nm,keys)
            if (AKCENT_KEYWORDS_MODE=="include" and not hit) or (AKCENT_KEYWORDS_MODE=="exclude" and hit):
                out_offers.remove(off); filtered+=1

    norm,dropped=ensure_vendor(out_shop)

    # Prices
    updated=0
    for off in out_offers.findall("offer"):
        d=get_dealer_price(off)
        if d: p=compute_price(d); 
        else: p=None
        if p:
            el=off.find("price") or ET.SubElement(off,"price")
            el.text=str(p); cur=off.find("currencyId") or ET.SubElement(off,"currencyId"); cur.text="KZT"; updated+=1

    avail=force_available(out_shop)
    offers_written=len(out_offers.findall("offer"))
    meta=f"""FEED_META
supplier = {SUPPLIER_NAME} | Метка поставщика
source   = {SUPPLIER_URL} | URL исходного XML
offers_total = {len(offers_in.findall('offer'))} | Офферов у поставщика до очистки
offers_written = {offers_written} | Офферов записано (после очистки)
keywords_mode = {AKCENT_KEYWORDS_MODE} | Режим фильтра
keywords_total = {len(keys)} | Ключей загружено
filtered_by_keywords = {filtered} | Отфильтровано по keywords
prices_updated = {updated} | Пересчитано цен
vendors_recovered = {norm} | Вендоров нормализовано
dropped_top = {','.join(dropped.keys()) if dropped else 'n/a'} | ТОП отброшенных брендов
available_forced = {avail} | Офферов получили available=true
built_utc = {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')} | Время сборки UTC
built_Asia/Almaty = {datetime.now(ZoneInfo('Asia/Almaty')).strftime('%Y-%m-%d %H:%M:%S %Z') if ZoneInfo else ''} | Время сборки Алматы
"""
    out_root.insert(0,ET.Comment(meta))
    ET.indent(out_root,space="  ")
    ET.ElementTree(out_root).write(OUT_FILE_YML,encoding=ENC,xml_declaration=True)
    ET.ElementTree(out_root).write(OUT_FILE_XML,encoding=ENC,xml_declaration=True)
    log(f"Wrote: {OUT_FILE_YML} & {OUT_FILE_XML} | offers={offers_written}")

if __name__=="__main__":
    main()
