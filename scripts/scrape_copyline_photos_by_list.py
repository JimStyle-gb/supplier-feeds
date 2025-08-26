from __future__ import annotations
import os, re, io, time, sys, urllib.parse
from typing import List, Optional, Tuple
import requests
from bs4 import BeautifulSoup
from xml.etree.ElementTree import Element, SubElement, ElementTree

BASE = "https://copyline.kz"
UA_HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# ===== ENV / параметры =====
ARTICLES_FILE = os.getenv("ARTICLES_FILE", "docs/copyline_articles.txt")
# Пишем прямо в docs/copyline.yml, чтобы отдавалось GitHub Pages
OUT_FILE      = os.getenv("OUT_FILE",      "docs/copyline.yml")
ENC           = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
REQUEST_DELAY = int(os.getenv("REQUEST_DELAY_MS", "1200")) / 1000.0

SEARCH_URLS = [
    "?searchword={q}&option=com_search&searchphrase=all",
    "search?searchword={q}",
    "index.php?option=com_search&searchword={q}",
]

def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+"," ", (s or "").strip())

def abs_url(url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return urllib.parse.urljoin(BASE + "/", url.lstrip("/"))

def fetch(url: str) -> str:
    r = requests.get(url, headers=UA_HEADERS, timeout=60)
    r.raise_for_status()
    return r.text

def read_articles(path: str) -> List[str]:
    arts=[]
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = norm(line)
                if not s or s.startswith("#"): continue
                for tok in re.split(r"[,;\s]+", s):
                    t = tok.strip()
                    if t:
                        arts.append(t)
    return arts

def extract_product_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    hrefs=set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.endswith(".html") and ("/goods/" in href or "/product/" in href):
            hrefs.add(abs_url(href))
    return list(hrefs)

def search_product_urls(article: str) -> List[str]:
    urls=[]
    for tpl in SEARCH_URLS:
        url = abs_url(tpl.format(q=urllib.parse.quote(article)))
        try:
            html = fetch(url)
            links = extract_product_links(html)
            urls.extend(links)
        except Exception:
            pass
        time.sleep(REQUEST_DELAY)
    seen=set(); uniq=[]
    for u in urls:
        if u not in seen:
            uniq.append(u); seen.add(u)
    uniq.sort(key=lambda u: (article not in u, len(u)))
    return uniq

def parse_main_image_from_product(url: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        html = fetch(url)
    except Exception:
        return None, None
    soup = BeautifulSoup(html, "lxml")
    title = soup.find("h1")
    name  = norm(title.get_text()) if title else None

    img = soup.find("img", attrs={"itemprop": "image"})
    if img and img.get("src"):
        return abs_url(img["src"]), name
    return None, name

# ===== YML =====
def build_yml(rows: List[dict]) -> bytes:
    root = Element("yml_catalog"); shop = SubElement(root, "shop")
    SubElement(shop, "name").text = "copyline-photos-list"
    curr = SubElement(shop, "currencies"); SubElement(curr, "currency", {"id":"KZT", "rate":"1"})
    cats = SubElement(shop, "categories"); SubElement(cats, "category", {"id": "9300000"}).text = "Copyline"
    offers = SubElement(shop, "offers")

    for it in rows:
        o = SubElement(offers, "offer", {
            "id": f"copyline:{it['article']}",
            "available": "true",
            "in_stock": "true",
        })
        SubElement(o, "name").text = it.get("name") or it["article"]
        SubElement(o, "currencyId").text = "KZT"
        SubElement(o, "categoryId").text = "9300000"
        SubElement(o, "vendorCode").text = it["article"]
        if it.get("picture"):
            SubElement(o, "picture").text = it["picture"]
        for tag in ("quantity_in_stock","stock_quantity","quantity"):
            SubElement(o, tag).text = "1"

    buf = io.BytesIO()
    ElementTree(root).write(buf, encoding=ENC, xml_declaration=True)
    return buf.getvalue()

def main():
    arts = read_articles(ARTICLES_FILE)
    if not arts:
        env_list = os.getenv("ARTICLES", "")
        arts = [a for a in re.split(r"[,;\s]+", env_list) if a.strip()]
    if not arts:
        print("ERROR: нет артикулов. Заполни docs/copyline_articles.txt или переменную окружения ARTICLES.", file=sys.stderr)
        sys.exit(1)

    out_rows=[]
    for i, art in enumerate(arts, 1):
        print(f"[{i}/{len(arts)}] {art} -> поиск...")
        links = search_product_urls(art)
        pic=None; name=None
        if links:
            for link in links:
                pic, name = parse_main_image_from_product(link)
                if pic: break
        out_rows.append({"article": art, "picture": pic, "name": name})
        time.sleep(REQUEST_DELAY)

    yml = build_yml(out_rows)
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "wb") as f:
        f.write(yml)

    have = sum(1 for r in out_rows if r.get("picture"))
    print(f"[DONE] articles={len(out_rows)} | pictures found={have} -> {OUT_FILE}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
