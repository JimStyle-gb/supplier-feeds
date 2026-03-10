# -*- coding: utf-8 -*-
    hits = [m.group(0).upper() for m in _NAME_MODEL_RE.finditer(n)]
    if not hits:
        return ""
    return hits[-1]


def build_offer(
    src: SourceOffer,
    *,
    schema_cfg: dict,
    vendor_blacklist: set[str],
    placeholder_picture: str,
    id_prefix: str = "AS",
) -> tuple[OfferOut | None, bool]:
    raw_id = norm_ws(src.raw_id)
    name = normalize_name(src.name)
    if not raw_id or not name:
        return None, False

    oid = build_offer_oid(raw_id, prefix=id_prefix)
    available = normalize_available(src.available_attr, src.available_tag)
    pictures = collect_picture_urls(src.picture_urls, placeholder_picture=placeholder_picture)
    vendor = normalize_vendor(src.vendor, vendor_blacklist=vendor_blacklist)

    desc_src = sanitize_native_desc(src.description or "", name=name)

    xml_params = collect_xml_params(src.offer_el, schema_cfg) if src.offer_el is not None else []
    desc_params = extract_desc_spec_pairs(desc_src, schema_cfg)
    params = merge_params(xml_params, desc_params)

    if not _has_param(params, "Модель"):
        inferred_model = _infer_model_from_name(name)
        if inferred_model:
            params.append(("Модель", inferred_model))

    price_in = normalize_price_in(src.purchase_price_text, src.price_text)
    price = compute_price(price_in)

    offer = OfferOut(
        oid=oid,
        available=available,
        name=name,
        price=price,
        pictures=pictures,
        vendor=vendor,
        params=params,
        native_desc=desc_src,
    )
    return offer, available


def build_offers(
    source_offers: list[SourceOffer],
    *,
    schema_cfg: dict,
    vendor_blacklist: set[str],
    placeholder_picture: str,
    id_prefix: str = "AS",
) -> tuple[list[OfferOut], int, int]:
    out: list[OfferOut] = []
    in_true = 0
    in_false = 0

    for src in source_offers:
        offer, available = build_offer(
            src,
            schema_cfg=schema_cfg,
            vendor_blacklist=vendor_blacklist,
            placeholder_picture=placeholder_picture,
            id_prefix=id_prefix,
        )
        if offer is None:
            continue
        if available:
            in_true += 1
        else:
            in_false += 1
        out.append(offer)

    out.sort(key=lambda x: x.oid)
    return out, in_true, in_false
