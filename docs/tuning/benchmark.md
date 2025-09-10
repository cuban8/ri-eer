# Benchmark Log (ri-eer)

## Dimensions
- Graph size: V / E
- Cluster: instance(s), count
- Runtime per stage: PR, CC, LPA
- Cost estimate: $/hr * hours
- Notes: skew, spill, retries

## Entries
| Date | Track | Cluster | V/E | Runtime | Cost | Notes |
|------|-------|---------|-----|---------|------|-------|
| 2025-09-10 | CPU | 10× r8g.48xl | 276M/440M | 7h 12m | ~$934 | spill in CC |
