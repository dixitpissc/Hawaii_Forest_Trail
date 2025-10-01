def _to_bool(v):
    if isinstance(v, bool): return v
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "t")

def _coerce_bool(v):
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):  return True
    if s in ("false", "0", "no"):  return False
    return None

def _coerce_float(v):
    try:
        if v is None or v == "" or str(v).lower() == "null":
            return None
        return float(v)
    except Exception:
        return None

def _value_str(v):
    if v is None or v == "" or str(v).lower() == "null":
        return None
    return str(v)


def add_txn_tax_detail_from_row(payload: dict, row) -> None:
    """
    Builds payload['TxnTaxDetail'] using header + up to 4 TaxLine components from Map_Invoice columns.
    Columns used (examples):
      - Mapped_TxnTaxCodeRef (preferred) or TxnTaxDetail_TxnTaxCodeRef_value
      - TxnTaxDetail_TotalTax
      - TxnTaxDetail_TaxLine_{i}__Amount
      - TxnTaxDetail_TaxLine_{i}__DetailType
      - TxnTaxDetail_TaxLine_{i}__TaxLineDetail_TaxRateRef_value
      - TxnTaxDetail_TaxLine_{i}__TaxLineDetail_PercentBased
      - TxnTaxDetail_TaxLine_{i}__TaxLineDetail_TaxPercent
      - TxnTaxDetail_TaxLine_{i}__TaxLineDetail_NetAmountTaxable
    """
    tax_detail = {}
    # Header-level tax code (prefer mapped)
    code = row.get("Mapped_TxnTaxCodeRef") or row.get("TxnTaxDetail_TxnTaxCodeRef_value")
    code_str = _value_str(code)
    if code_str:
        tax_detail["TxnTaxCodeRef"] = {"value": code_str}

    # Optional header total tax
    total_tax = _coerce_float(row.get("TxnTaxDetail_TotalTax"))
    if total_tax is not None:
        tax_detail["TotalTax"] = total_tax

    # Tax lines (0..3 based on your schema)
    tax_lines = []
    for i in range(4):
        amt = _coerce_float(row.get(f"TxnTaxDetail_TaxLine_{i}__Amount"))
        dtyp = _value_str(row.get(f"TxnTaxDetail_TaxLine_{i}__DetailType"))
        # If neither amount nor detail type exists, skip this slot
        if amt is None and not dtyp:
            continue

        line = {}
        # QBO expects "DetailType": "TaxLineDetail" for tax lines; use source if provided, else default correctly
        line["DetailType"] = dtyp if dtyp else "TaxLineDetail"
        if amt is not None:
            line["Amount"] = amt

        # Build inner TaxLineDetail
        inner = {}
        net_taxable = _coerce_float(row.get(f"TxnTaxDetail_TaxLine_{i}__TaxLineDetail_NetAmountTaxable"))
        if net_taxable is not None:
            inner["NetAmountTaxable"] = net_taxable

        tax_percent = _coerce_float(row.get(f"TxnTaxDetail_TaxLine_{i}__TaxLineDetail_TaxPercent"))
        if tax_percent is not None:
            inner["TaxPercent"] = tax_percent

        rate_ref = row.get("Mapped_TxnTaxRateRef")
        if rate_ref:
            inner["TaxRateRef"] = {"value": rate_ref}

        percent_based = _coerce_bool(row.get(f"TxnTaxDetail_TaxLine_{i}__TaxLineDetail_PercentBased"))
        if percent_based is not None:
            inner["PercentBased"] = percent_based

        if inner:
            line["TaxLineDetail"] = inner

        # Only append if we have a meaningful line (Amount or a non-empty inner)
        if ("Amount" in line) or ("TaxLineDetail" in line):
            tax_lines.append(line)

    if tax_lines:
        tax_detail["TaxLine"] = tax_lines

    # Attach only if we built anything (header code, total, or at least one line)
    if tax_detail:
        payload["TxnTaxDetail"] = tax_detail
