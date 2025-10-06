import argparse
import pandas as pd
from typing import Dict, Any, List

DECISION_ACCEPTED = "ACCEPTED"
DECISION_IN_REVIEW = "IN_REVIEW"
DECISION_REJECTED = "REJECTED"

DEFAULT_CONFIG = {
    "amount_thresholds": {
        "digital": 2500,
        "physical": 6000,
        "subscription": 1500,
        "_default": 4000
    },
    "latency_ms_extreme": 2500,
    "chargeback_hard_block": 2,
    "score_weights": {
        "ip_risk": {"low": 0, "medium": 2, "high": 4},
        "email_risk": {"low": 0, "medium": 1, "high": 3, "new_domain": 2},
        "device_fingerprint_risk": {"low": 0, "medium": 2, "high": 4},
        "user_reputation": {"trusted": -2, "recurrent": -1, "new": 0, "high_risk": 4},
        "night_hour": 1,
        "geo_mismatch": 2,
        "high_amount": 2,
        "latency_extreme": 2,
        "new_user_high_amount": 2,
    },
    "score_to_decision": {
        "reject_at": 10,
        "review_at": 4
    }
}

# Optional: override thresholds via environment variables (for CI/CD / canary tuning)
try:
    import os as _os
    _rej = _os.getenv("REJECT_AT")
    _rev = _os.getenv("REVIEW_AT")
    if _rej is not None:
        DEFAULT_CONFIG["score_to_decision"]["reject_at"] = int(_rej)
    if _rev is not None:
        DEFAULT_CONFIG["score_to_decision"]["review_at"] = int(_rev)
except Exception:
    pass

def is_night(hour: int) -> bool:
    return hour >= 22 or hour <= 5

def high_amount(amount: float, product_type: str, thresholds: Dict[str, Any]) -> bool:
    t = thresholds.get(product_type, thresholds.get("_default"))
    return amount >= t

def _check_hard_block(row: pd.Series, cfg: Dict[str, Any]) -> tuple[bool, List[str]]:
    chargeback_count = int(row.get("chargeback_count", 0))
    ip_risk = str(row.get("ip_risk", "low")).lower()
    
    if chargeback_count >= cfg["chargeback_hard_block"] and ip_risk == "high":
        return True, ["hard_block:chargebacks>=2+ip_high"]
    return False, []

def _assess_categorical_risks(row: pd.Series, cfg: Dict[str, Any]) -> tuple[int, List[str]]:
    score = 0
    reasons = []
    
    risk_fields = [
        ("ip_risk", cfg["score_weights"]["ip_risk"]),
        ("email_risk", cfg["score_weights"]["email_risk"]),
        ("device_fingerprint_risk", cfg["score_weights"]["device_fingerprint_risk"])
    ]
    
    for field, mapping in risk_fields:
        value = str(row.get(field, "low")).lower()
        points = mapping.get(value, 0)
        
        if points:
            score += points
            reasons.append(f"{field}:{value}(+{points})")
    
    return score, reasons

def _assess_user_reputation(row: pd.Series, cfg: Dict[str, Any]) -> tuple[int, List[str]]:
    reputation = str(row.get("user_reputation", "new")).lower()
    points = cfg["score_weights"]["user_reputation"].get(reputation, 0)
    
    reasons = []
    if points:
        sign = '+' if points >= 0 else ''
        reasons.append(f"user_reputation:{reputation}({sign}{points})")
    
    return points, reasons

def _assess_temporal_risk(row: pd.Series, cfg: Dict[str, Any]) -> tuple[int, List[str]]:
    hour = int(row.get("hour", 12))
    
    if is_night(hour):
        points = cfg["score_weights"]["night_hour"]
        return points, [f"night_hour:{hour}(+{points})"]
    
    return 0, []

def _assess_geographical_risk(row: pd.Series, cfg: Dict[str, Any]) -> tuple[int, List[str]]:
    bin_country = str(row.get("bin_country", "")).upper()
    ip_country = str(row.get("ip_country", "")).upper()
    
    if bin_country and ip_country and bin_country != ip_country:
        points = cfg["score_weights"]["geo_mismatch"]
        return points, [f"geo_mismatch:{bin_country}!={ip_country}(+{points})"]
    
    return 0, []

def _assess_amount_risk(row: pd.Series, cfg: Dict[str, Any]) -> tuple[int, List[str]]:
    amount = float(row.get("amount_mxn", 0.0))
    product_type = str(row.get("product_type", "_default")).lower()
    reputation = str(row.get("user_reputation", "new")).lower()
    
    score = 0
    reasons = []
    
    if high_amount(amount, product_type, cfg["amount_thresholds"]):
        points = cfg["score_weights"]["high_amount"]
        score += points
        reasons.append(f"high_amount:{product_type}:{amount}(+{points})")
        
        if reputation == "new":
            extra_points = cfg["score_weights"]["new_user_high_amount"]
            score += extra_points
            reasons.append(f"new_user_high_amount(+{extra_points})")
    
    return score, reasons

def _assess_latency_risk(row: pd.Series, cfg: Dict[str, Any]) -> tuple[int, List[str]]:
    latency = int(row.get("latency_ms", 0))
    
    if latency >= cfg["latency_ms_extreme"]:
        points = cfg["score_weights"]["latency_extreme"]
        return points, [f"latency_extreme:{latency}ms(+{points})"]
    
    return 0, []

def _apply_frequency_buffer(row: pd.Series, score: int, reasons: List[str]) -> tuple[int, List[str]]:
    reputation = str(row.get("user_reputation", "new")).lower()
    frequency = int(row.get("customer_txn_30d", 0))
    
    if reputation in ("recurrent", "trusted") and frequency >= 3 and score > 0:
        score -= 1
        reasons.append("frequency_buffer(-1)")
    
    return score, reasons

def _determine_decision(score: int, cfg: Dict[str, Any]) -> str:
    if score >= cfg["score_to_decision"]["reject_at"]:
        return DECISION_REJECTED
    elif score >= cfg["score_to_decision"]["review_at"]:
        return DECISION_IN_REVIEW
    else:
        return DECISION_ACCEPTED
    
def assess_row(row: pd.Series, cfg: Dict[str, Any]) -> Dict[str, Any]:
    is_blocked, block_reasons = _check_hard_block(row, cfg)
    if is_blocked:
        return {
            "decision": DECISION_REJECTED,
            "risk_score": 100,
            "reasons": ";".join(block_reasons)
        }
    
    score = 0
    reasons = []
    
    assessments = [
        _assess_categorical_risks(row, cfg),
        _assess_user_reputation(row, cfg),
        _assess_temporal_risk(row, cfg),
        _assess_geographical_risk(row, cfg),
        _assess_amount_risk(row, cfg),
        _assess_latency_risk(row, cfg)
    ]
    
    for points, assessment_reasons in assessments:
        score += points
        reasons.extend(assessment_reasons)
    
    score, reasons = _apply_frequency_buffer(row, score, reasons)
    
    decision = _determine_decision(score, cfg)
    
    return {
        "decision": decision,
        "risk_score": int(score),
        "reasons": ";".join(reasons)
    }

def run(input_csv: str, output_csv: str, config: Dict[str, Any] = None) -> pd.DataFrame:
    cfg = config or DEFAULT_CONFIG
    df = pd.read_csv(input_csv)
    results = []
    for _, row in df.iterrows():
        res = assess_row(row, cfg)
        results.append(res)
    out = df.copy()
    out["decision"] = [r["decision"] for r in results]
    out["risk_score"] = [r["risk_score"] for r in results]
    out["reasons"] = [r["reasons"] for r in results]
    out.to_csv(output_csv, index=False)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=False, default="transactions_examples.csv", help="Path to input CSV")
    ap.add_argument("--output", required=False, default="decisions.csv", help="Path to output CSV")
    args = ap.parse_args()
    out = run(args.input, args.output)
    print(out.head().to_string(index=False))

if __name__ == "__main__":
    main()
