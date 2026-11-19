"""Healing layer: turns a detection incident into a reviewed, validated proposal.

Deterministic code assembles the case, measures a baseline, and validates the
result; an LLM agent (claude_agent_sdk) only diagnoses and patches STAGED
copies. Nothing touches dags/ or manifests/ until a human runs `apply`.
"""
