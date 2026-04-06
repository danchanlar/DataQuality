def deduplicate(candidates, existing):
    return [c for c in candidates if c not in existing]

