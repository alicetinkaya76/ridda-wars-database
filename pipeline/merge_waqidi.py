#!/usr/bin/env python3
"""
MERGE WĀQIDĪ EVENTS INTO RIDDA DATABASE
==========================================
Takes the raw extraction output from ridda_pipeline.py for al-Wāqidī
and merges it into the existing enriched dataset (ridda_combined_enriched.json).

Key steps:
1. Load Wāqidī extraction output (ridda_waqidi.json)
2. Assign W001, W002, ... event IDs
3. Assign _source_name = "al-Wāqidī"
4. Run deduplication against existing Ṭabarī + Balādhurī events
5. Mark cross-referenced events
6. Run enrichment (geocoding, name normalization)
7. Merge into ridda_combined_enriched.json
8. Output updated dataset

Deduplication logic:
  An event from Wāqidī is flagged as a CROSS-REFERENCE (not a duplicate)
  if it matches an existing event on:
    - Same tribe (normalized) AND same region AND same year_ah
    - OR same rebel_leader AND same region
  Cross-referenced events are KEPT in the database with _cross_refs links,
  because having three independent source attestations strengthens the data.

Usage:
    python merge_waqidi.py --waqidi-input output/llm_results/ridda_waqidi.json \
                           --existing-data data/ridda_combined_enriched.json \
                           --output-dir data/
"""

import json
import argparse
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Optional, Tuple


# ============================================================
# NORMALIZATION HELPERS
# ============================================================

def normalize_arabic(text: str) -> str:
    """Normalize Arabic text for comparison."""
    if not text:
        return ''
    # Remove diacritics
    text = re.sub(r'[\u064B-\u065F\u0670]', '', text)
    # Normalize alef variants
    text = text.replace('أ', 'ا').replace('إ', 'ا').replace('آ', 'ا')
    # Normalize ta marbuta
    text = text.replace('ة', 'ه')
    # Remove prefixes for comparison
    text = re.sub(r'^(ال|بنو |بني |بن )', '', text.strip())
    return text.strip()


def normalize_english(text: str) -> str:
    """Normalize English transliteration for comparison."""
    if not text:
        return ''
    text = text.lower().strip()
    # Remove diacritical marks from transliteration
    replacements = {
        'ā': 'a', 'ī': 'i', 'ū': 'u',
        'ṭ': 't', 'ḥ': 'h', 'ḍ': 'd', 'ṣ': 's', 'ẓ': 'z',
        'ʿ': '', 'ʾ': '', ''': '', ''': '',
        'š': 'sh', 'ğ': 'gh', 'ḫ': 'kh',
    }
    for k, v in replacements.items():
        text = text.replace(k, v)
    # Remove common prefixes
    text = re.sub(r'^(al-|banu |bani |ibn )', '', text)
    return text.strip()


# ============================================================
# DEDUPLICATION / CROSS-REFERENCE MATCHING
# ============================================================

def compute_match_key(event: dict) -> str:
    """Compute a normalized match key for an event."""
    tribe = normalize_english(event.get('tribe_english', ''))
    if not tribe:
        tribe = normalize_arabic(event.get('tribe_arabic', ''))
    
    region = normalize_english(event.get('region_english', ''))
    if not region:
        region = normalize_arabic(event.get('region_arabic', ''))
    
    year = str(event.get('year_ah', ''))
    
    return f"{tribe}|{region}|{year}"


def compute_leader_key(event: dict) -> str:
    """Compute a rebel-leader match key."""
    leader = normalize_english(event.get('rebel_leader_english', ''))
    if not leader:
        leader = normalize_arabic(event.get('rebel_leader_arabic', ''))
    
    region = normalize_english(event.get('region_english', ''))
    if not region:
        region = normalize_arabic(event.get('region_arabic', ''))
    
    return f"{leader}|{region}" if leader else ''


def find_cross_references(waqidi_events: list, existing_events: list) -> Dict[int, List[str]]:
    """
    Find which Wāqidī events match existing Ṭabarī/Balādhurī events.
    Returns: {waqidi_index: [list of matching existing event IDs]}
    """
    # Build lookup from existing events
    existing_by_key = defaultdict(list)
    existing_by_leader = defaultdict(list)
    
    for e in existing_events:
        eid = e.get('_event_id', '')
        key = compute_match_key(e)
        if key and key != '||':
            existing_by_key[key].append(eid)
        
        lkey = compute_leader_key(e)
        if lkey and '|' in lkey and lkey.split('|')[0]:
            existing_by_leader[lkey].append(eid)
    
    # Match Wāqidī events
    cross_refs = {}
    for i, we in enumerate(waqidi_events):
        matches = set()
        
        # Try tribe+region+year match
        wkey = compute_match_key(we)
        if wkey in existing_by_key:
            matches.update(existing_by_key[wkey])
        
        # Try leader+region match
        wlkey = compute_leader_key(we)
        if wlkey and wlkey in existing_by_leader:
            matches.update(existing_by_leader[wlkey])
        
        if matches:
            cross_refs[i] = sorted(matches)
    
    return cross_refs


# ============================================================
# ENRICHMENT (MINIMAL — coordinates + names)
# ============================================================

# Coordinates for common regions
COORDINATES = {
    'اليمن': {'lat': 15.35, 'lon': 44.20, 'region_key': 'yaman', 'region_en': 'al-Yaman (Yemen)'},
    'نجد': {'lat': 24.50, 'lon': 45.50, 'region_key': 'najd', 'region_en': 'Najd'},
    'اليمامة': {'lat': 24.00, 'lon': 46.70, 'region_key': 'yamama', 'region_en': 'al-Yamāma'},
    'البحرين': {'lat': 26.00, 'lon': 50.50, 'region_key': 'bahrayn', 'region_en': 'al-Baḥrayn'},
    'حضرموت': {'lat': 15.90, 'lon': 49.00, 'region_key': 'hadramawt', 'region_en': 'Ḥaḍramawt'},
    'عمان': {'lat': 23.00, 'lon': 57.00, 'region_key': 'uman', 'region_en': 'ʿUmān (Oman)'},
    'الشام': {'lat': 33.50, 'lon': 36.30, 'region_key': 'sham', 'region_en': 'al-Shām'},
    'تهامة': {'lat': 14.80, 'lon': 43.30, 'region_key': 'tihama', 'region_en': 'Tihāma'},
    'مهرة': {'lat': 16.50, 'lon': 52.00, 'region_key': 'mahra', 'region_en': 'Mahra'},
    'المدينة': {'lat': 24.47, 'lon': 39.61, 'region_key': 'madina', 'region_en': 'al-Madīna'},
    'دبا': {'lat': 25.62, 'lon': 56.26, 'region_key': 'uman', 'region_en': 'Dabā (ʿUmān)'},
    'عقرباء': {'lat': 24.00, 'lon': 46.70, 'region_key': 'yamama', 'region_en': 'ʿAqrabāʾ (al-Yamāma)'},
    'بزاخة': {'lat': 27.50, 'lon': 41.50, 'region_key': 'najd', 'region_en': 'Buzākha (Najd)'},
    'البطاح': {'lat': 26.50, 'lon': 44.00, 'region_key': 'najd', 'region_en': 'al-Buṭāḥ (Najd)'},
}


def enrich_waqidi_event(event: dict) -> dict:
    """Add geocoding and metadata to a Wāqidī event."""
    # Try to geocode from region_arabic
    region_ar = event.get('region_arabic', '')
    battle_ar = event.get('battle_site_arabic', '')
    
    # Try battle site first, then region
    for loc in [battle_ar, region_ar]:
        if loc and loc in COORDINATES:
            coords = COORDINATES[loc]
            event['_lat'] = coords['lat']
            event['_lon'] = coords['lon']
            event['_region_key'] = coords['region_key']
            event['_region_en'] = coords['region_en']
            break
    
    # Set source metadata
    event['_source'] = 'waqidi'
    event['_source_name'] = 'al-Wāqidī'
    
    return event


# ============================================================
# MAIN MERGE FUNCTION
# ============================================================

def merge_waqidi(waqidi_path: str, existing_path: str, output_dir: str):
    """Merge Wāqidī extraction into existing dataset."""
    
    waqidi_path = Path(waqidi_path)
    existing_path = Path(existing_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("MERGE WĀQIDĪ INTO RIDDA DATABASE")
    print("=" * 60)
    
    # ---- Load Wāqidī extraction ----
    print(f"\n📖 Loading Wāqidī extraction: {waqidi_path}")
    with open(waqidi_path, 'r', encoding='utf-8') as f:
        waqidi_data = json.load(f)
    
    waqidi_events = waqidi_data.get('events', [])
    print(f"   Raw events: {len(waqidi_events)}")
    
    # ---- Load existing dataset ----
    print(f"\n📖 Loading existing dataset: {existing_path}")
    with open(existing_path, 'r', encoding='utf-8') as f:
        existing_data = json.load(f)
    
    existing_events = existing_data.get('events', [])
    tabari_count = sum(1 for e in existing_events if e.get('_source') == 'tabari')
    baladhuri_count = sum(1 for e in existing_events if e.get('_source') == 'baladhuri')
    print(f"   Existing events: {len(existing_events)} (T: {tabari_count}, B: {baladhuri_count})")
    
    # ---- Assign event IDs ----
    print(f"\n🔢 Assigning event IDs (W001, W002, ...)...")
    for i, evt in enumerate(waqidi_events, start=1):
        evt['_event_id'] = f'W{i:03d}'
    
    # ---- Enrich Wāqidī events ----
    print(f"\n🌍 Enriching Wāqidī events...")
    geocoded = 0
    for evt in waqidi_events:
        enrich_waqidi_event(evt)
        if evt.get('_lat'):
            geocoded += 1
    print(f"   Geocoded: {geocoded}/{len(waqidi_events)}")
    
    # ---- Find cross-references ----
    print(f"\n🔗 Finding cross-references with Ṭabarī/Balādhurī...")
    cross_refs = find_cross_references(waqidi_events, existing_events)
    
    xref_count = len(cross_refs)
    unique_count = len(waqidi_events) - xref_count
    print(f"   Cross-referenced: {xref_count} (matching existing events)")
    print(f"   Unique to Wāqidī: {unique_count}")
    
    # Report cross-references
    if cross_refs:
        print(f"\n   Cross-reference details:")
        for wi, match_ids in sorted(cross_refs.items()):
            we = waqidi_events[wi]
            tribe = we.get('tribe_english', we.get('tribe_arabic', '?'))
            region = we.get('region_english', we.get('region_arabic', '?'))
            print(f"   {we['_event_id']} ({tribe}, {region}) ↔ {', '.join(match_ids)}")
    
    # ---- Apply cross-references to events ----
    for wi, match_ids in cross_refs.items():
        waqidi_events[wi]['_cross_refs'] = match_ids
        waqidi_events[wi]['_is_cross_ref'] = True
        
        # Also update existing events to reference Wāqidī
        for eid in match_ids:
            for e in existing_events:
                if e.get('_event_id') == eid:
                    if '_cross_refs' not in e:
                        e['_cross_refs'] = []
                    e['_cross_refs'].append(waqidi_events[wi]['_event_id'])
    
    # ---- Merge ----
    print(f"\n📦 Merging datasets...")
    all_events = existing_events + waqidi_events
    
    total = len(all_events)
    new_tabari = sum(1 for e in all_events if e.get('_source') == 'tabari')
    new_baladhuri = sum(1 for e in all_events if e.get('_source') == 'baladhuri')
    new_waqidi = sum(1 for e in all_events if e.get('_source') == 'waqidi')
    
    print(f"   Total events: {total}")
    print(f"     al-Ṭabarī:    {new_tabari}")
    print(f"     al-Balādhurī:  {new_baladhuri}")
    print(f"     al-Wāqidī:     {new_waqidi}")
    print(f"     Cross-refs:    {xref_count} (events attested in multiple sources)")
    
    # ---- Update metadata ----
    existing_data['events'] = all_events
    existing_data['statistics'] = {
        'total_events': total,
        'by_source': {
            'tabari': new_tabari,
            'baladhuri': new_baladhuri,
            'waqidi': new_waqidi,
        },
        'cross_references': xref_count,
        'unique_waqidi': unique_count,
        'merged_at': datetime.now().isoformat(),
    }
    
    # Add Wāqidī to sources metadata
    if 'sources' in existing_data:
        existing_data['sources']['waqidi'] = {
            'name': 'al-Wāqidī',
            'name_ar': 'الواقدي',
            'full_title': 'Kitāb al-Ridda',
            'full_title_ar': 'كتاب الردة',
            'death_year': 207,
            'structure': 'narrative',
            'event_count': new_waqidi,
            'notes': 'Dedicated Ridda monograph. Earliest of three sources. Ibn al-Aʿtham redaction.'
        }
    
    existing_data['version'] = '2.0'
    
    # ---- Save outputs ----
    # Enriched JSON (updated)
    enriched_json = output_dir / 'ridda_combined_enriched.json'
    with open(enriched_json, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Saved: {enriched_json}")
    
    # Enriched CSV (updated)
    enriched_csv = output_dir / 'ridda_combined_enriched.csv'
    import csv
    csv_fields = [
        'event_id', 'source', 'source_name',
        'tribe_arabic', 'tribe_english', 'tribe_normalized',
        'region_arabic', 'region_english', 'region_key', 'region_en',
        'lat', 'lon',
        'year_ah', 'incorporation_mode',
        'commander_arabic', 'commander_english',
        'rebel_leader_arabic', 'rebel_leader_english',
        'battle_site_arabic', 'battle_site_english',
        'confidence', 'notes', 'evidence', 'cross_refs', 'is_cross_ref',
    ]
    with open(enriched_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields, extrasaction='ignore')
        writer.writeheader()
        for e in all_events:
            evidence = e.get('evidence', [])
            if isinstance(evidence, list):
                evidence = ' | '.join(evidence)
            xrefs = e.get('_cross_refs', [])
            if isinstance(xrefs, list):
                xrefs = ', '.join(xrefs)
            
            writer.writerow({
                'event_id': e.get('_event_id', ''),
                'source': e.get('_source', ''),
                'source_name': e.get('_source_name', ''),
                'tribe_arabic': e.get('tribe_arabic', ''),
                'tribe_english': e.get('tribe_english', ''),
                'tribe_normalized': e.get('_tribe_normalized', ''),
                'region_arabic': e.get('region_arabic', ''),
                'region_english': e.get('region_english', ''),
                'region_key': e.get('_region_key', ''),
                'region_en': e.get('_region_en', ''),
                'lat': e.get('_lat', ''),
                'lon': e.get('_lon', ''),
                'year_ah': e.get('year_ah', ''),
                'incorporation_mode': e.get('incorporation_mode', ''),
                'commander_arabic': e.get('commander_arabic', ''),
                'commander_english': e.get('commander_english', ''),
                'rebel_leader_arabic': e.get('rebel_leader_arabic', ''),
                'rebel_leader_english': e.get('rebel_leader_english', ''),
                'battle_site_arabic': e.get('battle_site_arabic', ''),
                'battle_site_english': e.get('battle_site_english', ''),
                'confidence': e.get('confidence', ''),
                'notes': e.get('notes', ''),
                'evidence': evidence,
                'cross_refs': xrefs,
                'is_cross_ref': e.get('_is_cross_ref', False),
            })
    print(f"✅ Saved: {enriched_csv}")
    
    # Wāqidī-only JSON (for reference)
    waqidi_json = output_dir / 'ridda_waqidi_enriched.json'
    with open(waqidi_json, 'w', encoding='utf-8') as f:
        json.dump({
            'source': 'waqidi',
            'source_name': 'al-Wāqidī',
            'count': len(waqidi_events),
            'cross_referenced': xref_count,
            'unique': unique_count,
            'events': waqidi_events,
        }, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved: {waqidi_json}")
    
    # ---- Summary ----
    print(f"\n{'='*60}")
    print("MERGE SUMMARY")
    print(f"{'='*60}")
    print(f"  Before:  {len(existing_events)} events (T:{tabari_count} + B:{baladhuri_count})")
    print(f"  Added:   {len(waqidi_events)} Wāqidī events")
    print(f"  After:   {total} events (T:{new_tabari} + B:{new_baladhuri} + W:{new_waqidi})")
    print(f"  X-refs:  {xref_count} events have multi-source attestation")
    print(f"{'='*60}")
    
    # Mode distribution
    from collections import Counter
    modes = Counter(e.get('incorporation_mode', '?') for e in all_events)
    print(f"\n  Mode distribution (all {total} events):")
    for mode, count in modes.most_common():
        print(f"    {mode}: {count} ({count/total*100:.1f}%)")
    
    # Source × Mode
    print(f"\n  Source × Mode:")
    for src in ['tabari', 'baladhuri', 'waqidi']:
        src_events = [e for e in all_events if e.get('_source') == src]
        src_modes = Counter(e.get('incorporation_mode', '?') for e in src_events)
        name = {'tabari': 'al-Ṭabarī', 'baladhuri': 'al-Balādhurī', 'waqidi': 'al-Wāqidī'}[src]
        print(f"    {name:15} ({len(src_events):3}): "
              f"SUB={src_modes.get('SUBJUGATION',0)} "
              f"MIX={src_modes.get('MIXED',0)} "
              f"SUM={src_modes.get('SUBMISSION',0)}")
    
    return total


def main():
    parser = argparse.ArgumentParser(description='Merge Wāqidī extraction into Ridda database')
    parser.add_argument('--waqidi-input', '-w', required=True,
                        help='Path to ridda_waqidi.json from pipeline')
    parser.add_argument('--existing-data', '-e', 
                        default='data/ridda_combined_enriched.json',
                        help='Path to existing enriched dataset')
    parser.add_argument('--output-dir', '-o', default='data/',
                        help='Output directory')
    args = parser.parse_args()
    
    merge_waqidi(args.waqidi_input, args.existing_data, args.output_dir)


if __name__ == '__main__':
    main()
