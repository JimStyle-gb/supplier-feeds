# replace in scripts/build_copyline.py

def best_header(ws):
    def score(arr):
        low=[norm(x).lower() for x in arr]
        got=set()
        for k,hints in HEADER_HINTS.items():
            for cell in low:
                if any(h in cell for h in hints):
                    got.add(k); break
        return len(got)

    rows=[]
    for row in ws.iter_rows(min_row=1, max_row=40, values_only=True):
        rows.append([norm("" if v is None else str(v)) for v in row])

    best_row, best_idx, best_sc = [], None, -1
    # одиночные строки
    for i,r in enumerate(rows):
        sc=score(r)
        if sc>best_sc:
            best_row, best_idx, best_sc = r, i+1, sc

    # склейка соседних строк; при равенстве — предпочитаем склейку
    for i in range(len(rows)-1):
        a,b=rows[i],rows[i+1]
        m=max(len(a),len(b)); merged=[]
        for j in range(m):
            x=a[j] if j<len(a) else ""
            y=b[j] if j<len(b) else ""
            merged.append((" ".join([x,y])).strip())
        sc=score(merged)
        if sc>best_sc or (sc==best_sc and sc>0):
            best_row, best_idx, best_sc = merged, i+2, sc

    return best_row, (best_idx or 1)+1
