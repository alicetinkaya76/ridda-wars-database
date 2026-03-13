# Ridda Wars Database — Codebook

## Fields
| Field | Type | Description |
|-------|------|-------------|
| event_id | string | Unique identifier (T001, W001, B001) |
| source | string | al-Tabari / al-Waqidi / al-Baladhuri |
| tribe_ar | string | Tribe name in Arabic |
| tribe_en | string | Tribe name (EI3 transliteration) |
| region | string | Geographic region (14 categories) |
| year_ah | int | 11 or 12 AH (null if undetermined) |
| mode | string | SUBJUGATION / MIXED / SUBMISSION |
| cause | string | false_prophet / zakat_refusal / tribal_autonomy / mixed_unclear |
| commander | string | Muslim commander (EI3) |
| rebel_leader | string | Rebel leader (EI3) |
| battle_site | string | Engagement location |
| lat | float | Latitude |
| lon | float | Longitude |
| evidence_ar | list | Arabic evidence passages |
| notes | string | Explanatory notes |
| confidence | float | 0.70–1.00 |

## Mode Criteria
- **SUBJUGATION:** قتال، قاتل، حرب، غزا، سيف، قتل، هزم، ظفر، غنم، سبي، فتح، حصار
- **SUBMISSION:** طاعة، أطاع، رجع، تاب، أسلم، بايع، صالح، أمان، سلم، أدى الزكاة
- **MIXED:** Both military and diplomatic elements in source text
