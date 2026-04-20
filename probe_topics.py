"""Compute topic0 for plausible UMA / Polymarket adapter events and
cross-reference against observed topic0s from probe_uma.py.

Usage:
    python probe_topics.py
"""
from web3 import Web3


# Observed from live probe.
OBSERVED = {
    "0x3f384afb4bd9f0aef0298c80399950011420eb33b0e1a750b20966270247b9a0": "UMA OOv2",
    "0x566c3fbdd12dd86bb341787f6d531f79fd7ad4ce7e3ae2d15ac0ca1b601af9df": "NegRiskUmaCtfAdapter",
}

# Candidate signatures. UMA OOv2 events from the actual verified contract.
CANDIDATES = [
    # UMA OOv2 (correct 10-arg ProposePrice)
    "RequestPrice(address,bytes32,uint256,bytes,address,uint256,uint256)",
    "ProposePrice(address,address,bytes32,uint256,bytes,int256,uint256,uint256,uint256,address)",
    "DisputePrice(address,address,address,bytes32,uint256,bytes,int256)",
    "Settle(address,address,address,bytes32,uint256,bytes,int256,uint256)",

    # UMA OOv2 (8-arg, older variants — what we had)
    "ProposePrice(address,address,bytes32,uint256,bytes,int256,uint256,uint256)",
    "ProposePrice(address,address,bytes32,uint256,bytes,int256,uint256)",

    # Polymarket UmaCtfAdapter
    "QuestionInitialized(bytes32,bytes,address,address,uint256,uint256)",
    "QuestionResolved(bytes32,uint256,uint256[])",
    "QuestionFlagged(bytes32)",
    "QuestionReset(bytes32)",
    "QuestionEmergencyResolved(bytes32,uint256[])",

    # Polymarket NegRiskUmaCtfAdapter
    "QuestionInitialized(bytes32,uint256,address,bytes,uint256,uint256,uint256)",
    "QuestionResolved(bytes32,int256)",
    "QuestionPaused(bytes32)",
    "QuestionUnpaused(bytes32)",
    "QuestionReset(bytes32)",
    "QuestionEmergencyResolved(bytes32,int256)",

    # Generic / fallback
    "PriceProposed(bytes32,int256,uint256)",
    "Proposed(bytes32,int256)",
]

print("Computing topic0 for each candidate event signature...\n")

hits = []
for sig in CANDIDATES:
    h = "0x" + Web3.keccak(text=sig).hex().lstrip("0x")
    match = OBSERVED.get(h)
    marker = f"  ← MATCHES {match}" if match else ""
    print(f"  {h}  {sig}{marker}")
    if match:
        hits.append((sig, h, match))

print()
print("=" * 70)
if hits:
    print("MATCHES FOUND:")
    for sig, h, match in hits:
        print(f"  {match:25} → {sig}")
        print(f"    topic0: {h}")
else:
    print("No matches. The events are probably different — we need to ABI-decode")
    print("from the full tx via eth_getTransactionReceipt to see the event name.")
