[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19008862.svg)](https://doi.org/10.5281/zenodo.19008862)

# Ridda Wars Database

Structured database of **138 tribal reincorporation events** during the Ridda Wars
(11–12 AH / 632–633 CE), extracted from three classical Arabic chronicles using a
Generative Information Extraction (GenIE) pipeline.

## Sources
- **al-Ṭabarī** (d. 310/923), *Tārīkh al-Rusul wa-l-Mulūk* — 60 events
- **al-Wāqidī** (d. 207/823), *Kitāb al-Ridda* — 47 events
- **al-Balādhurī** (d. 279/892), *Futūḥ al-Buldān* — 31 events

## Dataset Summary
| Metric | Value |
|--------|-------|
| Total source-event records | 138 |
| Deduplicated episodes | 102 |
| Cross-referenced events | 36 |
| Geocoded events | 137/138 (99.3%) |
| Unique tribal designations | 55 |
| Named commanders | 36 |
| Geographic regions | 14 |

## Incorporation Modes
- **SUBJUGATION** (*qitāl*): 93 events (67.4%)
- **MIXED**: 22 events (15.9%)
- **SUBMISSION** (*ṭāʿa*): 23 events (16.7%)

## Validation
- Three-rater Fleiss' κ (external LLMs) = 0.762 for incorporation mode
- Five-way Fleiss' κ (4 LLMs + human expert) = 0.730
- Field-level accuracy: tribe 100%, region 100%, year 100%, commander 93.3%

## Repository Structure
```
ridda-wars-database/
├── data/           # Dataset (JSON, CSV, XLSX, source texts)
├── pipeline/       # Extraction, enrichment, validation scripts
├── prompts/        # GenIE prompt templates
├── validation/     # Cross-model annotation results
├── figures/        # Publication figures and generator
├── lexicon/        # Arabic mode classification lexicon
└── docs/           # Codebook and field definitions
```

## Citation
> Gökalp, Hüseyin, and Ali Çetinkaya. 2026. "Mapping Tribal Reincorporation in the
> Ridda Wars (11–12 AH): A Database-Driven Analysis of al-Ṭabarī, al-Wāqidī, and
> al-Balādhurī." *Religions* 17(X): XXX.

## License
MIT License

## Acknowledgments
Source texts from [OpenITI](https://github.com/OpenITI).
Geocoding via [al-Ṯurayyā Gazetteer](https://althurayya.github.io/).
