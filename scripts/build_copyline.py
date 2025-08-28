# -*- coding: utf-8 -*-
"""
Copyline feed builder:
- Read XLSX (auto-detect header row on each sheet; find Name/Price/SKU)
- Keep only items whose NAME starts with a keyword (allow leading brand like 'ripo ')
- Crawl product pages for picture, full description, breadcrumbs
- Emit YML for Satu
"""

import os, re, io, time, html, hashlib, random
from typing import List, Dict, Tuple, Optional
import requests, pandas as pd
from bs4 import BeautifulSoup

BASE = "https://copyline.kz"
XLSX_URL = os.environ.get("XLSX_URL", f"{BASE}/files/price-CLA.xlsx")
KEYWORDS_FILE = os.environ.get("KEYWORDS_FILE", "docs/copyline_keywords.txt")
OUT_FILE = os.environ.get("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING = os.environ.get("OUTPUT_ENCODING", "windows-1251")
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS = int(os.environ.get("REQUEST_DELAY_MS", "120"))
MIN_BYTES = int(os.environ.get("MIN_BYTES", "900"))
ALLOW_PREFIX_BRANDS = [w.strip().lower() for w in os.environ.get(
    "ALLOW_PREFIX_BRANDS",
    "ripo,hp,canon,samsung,xerox,brother,pantum,lexmark,kyocera,konica,minolta,ricoh,panasonic"
).split(",") if w.strip()]
MAX_SITEMAP_URLS = int(os.environ.get("MAX_SITEMAP_URLS", "12000"))
MAX_VISIT_PAGES = int(os.environ.get("MAX_VISIT_PAGES", "2500"))

ROOT_CAT_ID = 9300000
ROOT_CAT_NAME = "Copyline"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36"

# ---------- utils ----------
def sleep_jitter(ms: int):
    base = ms / 1000.0
    time.sleep(max(0.0, base + random.uniform(-0.12, 0.12)*base))

def http_get(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            print(f"[warn] {r.status_code} {url}")
            return None
        if len(r.content) < MIN_BYTES:
            print(f"[warn] too small {len(r.content)}B {url}")
            return None
        return r.content
    except Exception as e:
        print(f"[err] GET {url} -> {e}")
        return None

def soup(html_bytes: bytes) -> BeautifulSoup:
    return BeautifulSoup(html_bytes, "html.parser")

def clean(s: str) -> str:
    s = html.unescape(s or "")
    return re.sub(r"\s+", " ", s).strip()

def normalize_img_to_full(u: str) -> str:
    if not u: return ""
    if u.startswith("//"): u = "https:" + u
    if u.startswith("/"): u = BASE + u
    m = re.match(r"^(https?://[^/]+)(/.*/)([^/]+)$", u)
    if not m: return u
    host, path, fname = m.groups()
    if fname.startswith("full_"): return u
    if fname.startswith("thumb_"): fname = "full_" + fname[len("thumb_"):]
    else: fname = "full_" + fname
    return f"{host}{path}{fname}"

# ---------- XLSX ----------
NAME_ALIASES = ["наименование товара","наименование","название","товар","продукт","номенклатура","позиция","описание","модель","item","product","name"]
PRICE_ALIASES = ["цена","цена, тг","цена тг","цена (тг)","цена, тг., с ндс","цена с ндс","розничная цена","стоимость","price","цена kzt","цена в тенге"]
SKU_ALIASES   = ["артикул","код","код товара","vendorcode","sku","part","партномер","pn"]

def _norm_head(x)->str:
    s = str(x or "").strip().lower()
    s = re.sub(r"\s+"," ", s).replace("ё","е")
    return s

def _num_series(sr: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(sr):
        return pd.to_numeric(sr, errors="coerce")
    s = sr.astype(str).str.strip()
    s = s.str.replace(r"[^\d,.\- ]", "", regex=True).str.replace(" ","", regex=False)
    def _fix(x):
        return x.replace(",", "") if ("," in x and "." in x) else x.replace(",", ".")
    s = s.apply(_fix)
    return pd.to_numeric(s, errors="coerce")

def _pick_alias(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    low = {_norm_head(c): c for c in df.columns}
    for a in aliases:
        an = _norm_head(a)
        for lc, real in low.items():
            if lc == an or lc.startswith(an):
                return real
    return None

def _guess_price(df: pd.DataFrame)->Optional[str]:
    best,score=None,-1.0
    for c in df.columns:
        serie = _num_series(df[c])
        pos = (serie>0).sum()
        sc = pos/max(1,len(serie))
        if sc>score:
            best,score=c,sc
    return best

def _guess_name(df: pd.DataFrame)->Optional[str]:
    best,score=None,-1.0
    for c in df.columns:
        s=df[c].astype(str)
        nonnum=(s.str.contains(r"[A-Za-zА-Яа-я]", regex=True)).mean()
        avg=s.str.len().mean()
        sc=nonnum*avg
        if sc>score:
            best,score=c,sc
    return best

def _reheader(raw: pd.DataFrame)->pd.DataFrame:
    """Try to find a header row within first ~10 rows; else return as-is with current header."""
    df=raw.copy()
    # if it already looks good -> return
    if _pick_alias(df, NAME_ALIASES) or _pick_alias(df, PRICE_ALIASES):
        return df
    scan=min(10,len(df))
    for i in range(scan):
        row = [str(x).strip() for x in df.iloc[i].tolist()]
        if any(_norm_head(x) for x in row):
            new = df.iloc[i+1:].copy()
            new.columns = row
            if _pick_alias(new, NAME_ALIASES) or _pick_alias(new, PRICE_ALIASES):
                return new
    return df

def read_xlsx(url:str)->pd.DataFrame:
    b=http_get(url)
    if not b: raise RuntimeError("Не удалось скачать XLSX.")
    x=pd.ExcelFile(io.BytesIO(b), engine="openpyxl")
    best=None; picked=None
    for sh in x.sheet_names:
        try:
            raw=x.parse(sheet_name=sh, header=0, dtype=object)
        except:
            raw=x.parse(sheet_name=sh, header=None, dtype=object)
        if raw is None or raw.empty: continue
        df=_reheader(raw).dropna(how="all",axis=0).dropna(how="all",axis=1)
        if df.empty: continue
        name_col=_pick_alias(df,NAME_ALIASES) or _guess_name(df)
        price_col=_pick_alias(df,PRICE_ALIASES) or _guess_price(df)
        sku_col=_pick_alias(df,SKU_ALIASES)
        if not name_col or not price_col:  # sheet not suitable
            continue
        tmp=pd.DataFrame({
            "name": df[name_col].astype(str).str.strip(),
            "price": _num_series(df[price_col]),
            "sku": (df[sku_col].astype(str) if sku_col else "")
        })
        tmp=tmp[(tmp["name"]!="") & (tmp["price"].fillna(0)>0)]
        if tmp.empty: continue
        if best is None or len(tmp)>len(best):
            best=tmp.reset_index(drop=True); picked=sh
    if best is None:
        raise RuntimeError("Нет обязательных столбцов 'Название'/'Цена' (ни на одном листе).")
    print(f"[xls] sheet: {picked}, rows: {len(best)}")
    return best

# ---------- keywords: startswith (+brand prefix allowed) ----------
def load_keywords(path:str)->List[str]:
    try:
        return [ln.strip() for ln in open(path,"r",encoding="utf-8") if ln.strip() and not ln.strip().startswith("#")]
    except FileNotFoundError:
        return []

def build_startswith_patterns(kws: List[str]) -> List[re.Pattern]:
    brand = ""
    if ALLOW_PREFIX_BRANDS:
        brand = r"(?:\s*(?:%s)\s+)?" % "|".join(re.escape(b) for b in ALLOW_PREFIX_BRANDS)
    pats=[]
    for kw in kws:
        kw=re.escape(kw.strip())
        # ^(optional brand) + keyword + word boundary or non-word breaker
        pats.append(re.compile(r"^\s*"+brand+kw+r"(?:\b|[^0-9A-Za-zА-Яа-я])", re.I))
    return pats

def startswith_keyword(name:str, pats:List[re.Pattern])->bool:
    s=name.strip()
    for p in pats:
        m=p.search(s)
        if m and m.start()==0:
            return True
    return False

# ---------- tokens / mapping ----------
def extract_tokens(text:str)->List[str]:
    t=str(text or "").upper()
    out=set()
    for m in re.findall(r"\b[A-ZА-Я0-9]{1,6}(?:-[A-ZА-Я0-9]{1,6}){0,3}\b", t):
        if any(ch.isdigit() for ch in m) and len(m)>=3:
            out.add(m)
    for m in re.findall(r"\bC-EXV\d{1,3}\b", t):
        out.add(m)
    return list(out)

def nrm(s:str)->str:
    return re.sub(r"[^0-9a-zа-я]", "", s.lower())

# ---------- site crawl ----------
def fetch_sitemap_urls()->List[str]:
    seen=set(); out=[]
    def grab(u:str):
        if u in seen or len(out)>=MAX_SITEMAP_URLS: return
        seen.add(u)
        b=http_get(u); 
        if not b: return
        sp=BeautifulSoup(b,"xml")
        for loc in sp.find_all("loc"):
            s=loc.get_text(strip=True)
            if not s: continue
            if s.endswith(".xml"): grab(s)
            else: out.append(s)
    grab(f"{BASE}/sitemap.xml")
    return list(dict.fromkeys(out))

def parse_product(url:str)->Optional[Dict]:
    sleep_jitter(REQUEST_DELAY_MS)
    b=http_get(url); 
    if not b: return None
    sp=soup(b)
    h1=sp.find("h1")
    name=clean(h1.get_text(" ",strip=True)) if h1 else clean(sp.title.get_text(" ",strip=True)) if sp.title else ""
    if not name: return None
    img = sp.find("img", attrs={"id": re.compile(r"^main_image_")}) or sp.find("img", attrs={"itemprop": "image"})
    if not img:
        for c in sp.find_all("img"):
            src=c.get("src") or c.get("data-src") or ""
            if "img_products" in src:
                img=c; break
        if not img:
            imgs=sp.find_all("img"); img=imgs[0] if imgs else None
    pic=""
    if img:
        src=img.get("src") or img.get("data-src") or ""
        pic=normalize_img_to_full(src)
    desc=""
    for cls in ["jshop_prod_description","product_description","prod_description","description"]:
        el=sp.find(True, class_=lambda c: c and cls in c)
        if el: 
            desc=clean(el.get_text(" ",strip=True)); 
            if desc: break
    if not desc:
        main=sp.find("div", {"id":"content"}) or sp.find("div", {"class": re.compile("content|product", re.I)})
        if main: desc=clean(main.get_text(" ",strip=True))[:4000]
    cats=[]
    bc=sp.find("ul", class_=re.compile("breadcrumb"))
    if bc:
        for a in bc.find_all("a"):
            t=clean(a.get_text(" ",strip=True))
            if t and t.lower() not in ("главная","home","наш каталог","наши товары"):
                cats.append(t)
    cats=[c for c in cats if len(c)>=2][:5]
    return {"url":url,"name":name,"picture":pic,"description":desc,"categories":cats}

def fetch_pages_for_tokens(tokens_norm:set)->Dict[str,Dict]:
    urls=[u for u in fetch_sitemap_urls() if "/goods/" in u][:MAX_SITEMAP_URLS]
    def ok(u:str)->bool:
        path=u.lower()
        pn=nrm(path)
        if any(t in pn for t in tokens_norm): return True
        for hint in ["drum","dr-","drunit","toner","cartridge","tn-","developer","cable","kabel","kab","patch","fuser","termoblock","termo","heater"]:
            if hint in path: return True
        return False
    cand=[u for u in urls if ok(u)][:MAX_VISIT_PAGES]
    print(f"[site] goods in sitemap: {len(urls)}, to visit: {len(cand)}")
    data={}
    for i,u in enumerate(cand,1):
        p=parse_product(u)
        if p and p.get("name"): data[u]=p
        if i%50==0: print(f"[site] parsed {i}/{len(cand)}")
    return data

# ---------- YML ----------
def slug(text:str)->str:
    t=text.strip().lower()
    t=re.sub(r"[^\w\s-]+","",t); t=re.sub(r"\s+","-",t); t=re.sub(r"-{2,}","-",t)
    return (t.strip("-")[:80] or "item")

def stable_id(seed:str, prefix:int=9400000)->int:
    h=hashlib.md5(seed.encode("utf-8")).hexdigest()[:6]
    return prefix+int(h,16)

def price_str(x:float)->str:
    return str(int(x)) if float(x).is_integer() else f"{x:.2f}".rstrip("0").rstrip(".")

def yml(categories: List[Tuple[int,str,int]], offers: List[Dict])->str:
    out=[]
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append("<name>copyline</name>")
    out.append("<currencies><currency id=\"KZT\" rate=\"1\" /></currencies>")
    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{html.escape(ROOT_CAT_NAME)}</category>")
    for cid,cname,parent in categories:
        out.append(f"<category id=\"{cid}\" parentId=\"{parent}\">{html.escape(cname)}</category>")
    out.append("</categories>")
    out.append("<offers>")
    for o in offers:
        oid=f"copyline:{slug(o['name'])}:{hashlib.md5((o['url']+o.get('sku','')).encode()).hexdigest()[:8]}"
        out.append(f"<offer id=\"{oid}\" available=\"true\" in_stock=\"true\">")
        out.append(f"<name>{html.escape(o['name'])}</name>")
        out.append("<vendor>Copyline</vendor>")
        if o.get("sku"): out.append(f"<vendorCode>{html.escape(o['sku'])}</vendorCode>")
        out.append(f"<price>{price_str(o['price'])}</price>")
        out.append("<currencyId>KZT</currencyId>")
        out.append(f"<categoryId>{o['categoryId']}</categoryId>")
        out.append(f"<url>{html.escape(o['url'])}</url>")
        if o.get("picture"): out.append(f"<picture>{html.escape(o['picture'])}</picture>")
        if o.get("description"): out.append(f"<description>{html.escape(o['description'])}</description>")
        out.append("<quantity_in_stock>1</quantity_in_stock><stock_quantity>1</stock_quantity><quantity>1</quantity>")
        out.append("</offer>")
    out.append("</offers></shop></yml_catalog>")
    return "\n".join(out)

# ---------- main ----------
def extract_tokens(text:str)->List[str]:
    t=str(text or "").upper()
    out=set()
    for m in re.findall(r"\b[A-ZА-Я0-9]{1,6}(?:-[A-ZА-Я0-9]{1,6}){0,3}\b", t):
        if any(ch.isdigit() for ch in m) and len(m)>=3:
            out.add(m)
    for m in re.findall(r"\bC-EXV\d{1,3}\b", t):
        out.add(m)
    return list(out)

def nrm(s:str)->str:
    return re.sub(r"[^0-9a-zа-я]", "", s.lower())

def main():
    # 1) XLSX
    df = read_xlsx(XLSX_URL)

    # 2) keywords
    kws = load_keywords(KEYWORDS_FILE)
    if not kws:
        raise RuntimeError("Файл ключей пуст. Заполни docs/copyline_keywords.txt")
    pats = build_startswith_patterns(kws)

    df["__n"]=df["name"].astype(str).str.strip()
    fdf=df[df["__n"].apply(lambda s: startswith_keyword(s, pats))].copy().drop(columns="__n")
    if fdf.empty:
        raise RuntimeError("После фильтрации по ключам не осталось товаров из XLSX.")

    # 3) collect tokens, crawl site
    tokens=set()
    for s in (fdf["name"].tolist()+fdf["sku"].tolist()):
        for t in extract_tokens(s): tokens.add(t)
    norm_tokens={nrm(t) for t in tokens if t.strip()}

    pages=fetch_pages_for_tokens(norm_tokens)
    if not pages:
        raise RuntimeError("Не нашли ни одной карточки на сайте.")

    token2urls={}
    for u,p in pages.items():
        for t in extract_tokens(p["name"]):
            token2urls.setdefault(nrm(t), []).append(u)

    # 4) categories
    cat_by_parent_name={}
    categories_list=[]  # tuples (cid, name, parent_id)

    def ensure_cat_path(path:List[str])->int:
        parent_id=ROOT_CAT_ID
        parent_name=ROOT_CAT_NAME
        for cname in path:
            key=(parent_id, cname)
            if key not in cat_by_parent_name:
                cid=stable_id(f"{parent_id}>{cname}", prefix=9400000)
                cat_by_parent_name[key]=cid
                categories_list.append((cid, cname, parent_id))
            parent_id = cat_by_parent_name[key]
            parent_name = cname
        return parent_id

    # 5) match rows to pages
    used=set()
    def pick_url(row: pd.Series)->Optional[str]:
        toks=extract_tokens(str(row.get("name",""))+" "+str(row.get("sku","")))
        cand=[]
        for t in toks:
            for u in token2urls.get(nrm(t), []): cand.append(u)
        if not cand:
            rn=str(row["name"]).lower()
            base=[kw.lower() for kw in kws if kw.lower() in rn][:2]
            for u,p in pages.items():
                h1=p["name"].lower()
                if all(k in h1 for k in base): cand.append(u)
        if not cand: return None
        name_low=str(row["name"]).lower()
        brand_hits=[b for b in ALLOW_PREFIX_BRANDS if b in name_low]
        def score(u:str)->int:
            sc=0
            for t in toks:
                if nrm(t) in nrm(u): sc+=len(t)
            for b in brand_hits:
                if b in u.lower(): sc+=3
            return sc
        cand=sorted(set(cand), key=lambda x:(-score(x), len(x)))
        for u in cand:
            if u not in used: return u
        return cand[0]

    offers=[]
    for _,row in fdf.iterrows():
        u=pick_url(row)
        if not u: continue
        p=pages.get(u); 
        if not p: continue
        # Check site title also starts with a keyword (+brand allowed)
        if not startswith_keyword(p["name"], pats): continue
        if not p.get("picture"): continue
        cat_path=p.get("categories") or ["Наши товары"]
        cat_id=ensure_cat_path(cat_path)
        offers.append({
            "name": p["name"],
            "price": float(row["price"]),
            "sku": str(row.get("sku","") or ""),
            "url": u,
            "picture": p["picture"],
            "description": p.get("description",""),
            "categoryId": cat_id
        })
        used.add(u)

    if not offers:
        raise RuntimeError("No matched items with photos after filtering.")

    # 6) write YML
    xml=yml(categories_list, offers)
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE,"w",encoding="cp1251",errors="ignore") as f:
        f.write(xml)
    print(f"[done] offers: {len(offers)} -> {OUT_FILE}")

if __name__=="__main__":
    main()
