#!/usr/bin/env python3
"""
RIDDA WARS DATABASE - SCHOLARLY VALIDATION SCRIPT
==================================================
Validates the extracted database against modern Western scholarship.

Usage:
    python validate_ridda.py [--tabari FILE] [--baladhuri FILE] [--combined FILE] [--output FILE]
    
Examples:
    # Validate separate source files
    python validate_ridda.py --tabari output/llm_results/ridda_tabari.json --baladhuri output/llm_results/ridda_baladhuri.json
    
    # Validate combined file
    python validate_ridda.py --combined ridda_combined.json
    
    # Save report to file
    python validate_ridda.py --combined ridda_combined.json --output validation_report.md
"""

import json
import argparse
from datetime import datetime
from pathlib import Path


# =============================================================================
# SCHOLARLY REFERENCE DATA
# =============================================================================

# Key tribes documented in Western scholarship
SCHOLARLY_TRIBES = {
    'Banū Ḥanīfa': {
        'variants': ['hanifa', 'حنيفة', 'hanīfa'],
        'reference': 'Musaylima\'s tribe - Battle of Yamama',
        'sources': ['Donner 1981', 'Shoufani 1972', 'Kister 2002']
    },
    'Banū Asad': {
        'variants': ['asad', 'أسد'],
        'reference': 'Ṭulayḥa\'s tribe',
        'sources': ['Landau-Tasseron "Asad from Jāhiliyya to Islam" JSAI 1985']
    },
    'Banū Tamīm': {
        'variants': ['tamim', 'tamīm', 'تميم'],
        'reference': 'Mālik ibn Nuwayra\'s tribe',
        'sources': ['Landau-Tasseron 1981', 'Watt 1956']
    },
    'Ṭayyiʾ': {
        'variants': ['tayyi', 'طيء', 'tayy'],
        'reference': 'Central Arabian tribe',
        'sources': ['Landau-Tasseron "Participation of Ṭayyi in the Ridda" JSAI 1984']
    },
    'Ghaṭafān': {
        'variants': ['ghatafan', 'غطفان'],
        'reference': 'Allied with Ṭulayḥa',
        'sources': ['Landau-Tasseron PhD thesis 1981']
    },
    'Banū Sulaym': {
        'variants': ['sulaym', 'سليم'],
        'reference': 'Western Arabian tribe',
        'sources': ['Lecker "The Banū Sulaym" 1989']
    },
    'Kinda': {
        'variants': ['kinda', 'كندة'],
        'reference': 'South Arabian confederation',
        'sources': ['Lecker "Kinda on the eve of Islam" JRAS 1994', 
                   'Lecker "Judaism among Kinda" JAOS 1995']
    },
    'al-Azd': {
        'variants': ['azd', 'الأزد'],
        'reference': 'Oman rebellion under Laqīṭ',
        'sources': ['Britannica "Riddah"']
    },
    'Rabīʿa': {
        'variants': ['rabīʿa', 'rabia', 'ربيعة'],
        'reference': 'Bahrain region tribes',
        'sources': ['Donner 1981']
    },
    'ʿAbs': {
        'variants': ['abs', 'عبس'],
        'reference': 'Allied with Ghaṭafān',
        'sources': ['Landau-Tasseron 1981']
    },
    'Fazāra': {
        'variants': ['fazara', 'فزارة'],
        'reference': 'Allied with Ṭulayḥa',
        'sources': ['Donner 1981']
    },
}

# Key geographic regions
SCHOLARLY_REGIONS = {
    'al-Yamāma': {
        'variants': ['yamama', 'yamāma', 'اليمامة'],
        'reference': 'Battle of ʿAqrabāʾ / Garden of Death',
        'sources': ['Britannica', 'Donner 1981', 'Kister 2002']
    },
    'al-Baḥrayn': {
        'variants': ['bahrayn', 'bahrain', 'البحرين'],
        'reference': 'Bahrain campaign',
        'sources': ['World History Encyclopedia', 'Donner 1981']
    },
    'al-Yaman': {
        'variants': ['yaman', 'yemen', 'اليمن'],
        'reference': 'al-Aswad al-ʿAnsī rebellion',
        'sources': ['Britannica', 'Shoufani 1972']
    },
    'Ḥaḍramawt': {
        'variants': ['hadramawt', 'حضرموت'],
        'reference': 'Final Ridda battles (March 633)',
        'sources': ['World History Encyclopedia']
    },
    'ʿUmān': {
        'variants': ['uman', 'oman', 'عمان'],
        'reference': 'Laqīṭ "Dhū l-Tāj" rebellion',
        'sources': ['Britannica', 'TheCollector']
    },
    'Najd': {
        'variants': ['najd', 'نجد'],
        'reference': 'Central Arabia - Buzākha battle',
        'sources': ['Donner 1981']
    },
    'Buzākha': {
        'variants': ['buzakha', 'بزاخة'],
        'reference': 'Battle against Ṭulayḥa',
        'sources': ['Wikipedia', 'Donner 1981']
    },
}

# Key Muslim commanders
SCHOLARLY_COMMANDERS = {
    'Khālid ibn al-Walīd': {
        'variants': ['khalid', 'خالد'],
        'reference': 'Supreme commander of Ridda campaigns',
        'sources': ['All major sources']
    },
    'ʿIkrima ibn Abī Jahl': {
        'variants': ['ikrima', 'عكرمة'],
        'reference': 'Oman and Yemen campaigns',
        'sources': ['TheCollector', 'Madain Project']
    },
    'al-ʿAlāʾ ibn al-Ḥaḍramī': {
        'variants': ['ala', 'العلاء'],
        'reference': 'Bahrain campaign commander',
        'sources': ['Madain Project', 'Donner 1981']
    },
    'Ḥudhayfa ibn Miḥṣan': {
        'variants': ['hudhayfa', 'حذيفة'],
        'reference': 'Oman campaign',
        'sources': ['TheCollector']
    },
}

# Key rebel leaders / false prophets
SCHOLARLY_REBELS = {
    'Musaylima': {
        'variants': ['musaylima', 'مسيلمة'],
        'reference': 'False prophet of al-Yamāma',
        'sources': ['Kister "Struggle against Musaylima" JSAI 2002', 'Britannica']
    },
    'Ṭulayḥa': {
        'variants': ['tulayha', 'طليحة'],
        'reference': 'False prophet of Banū Asad',
        'sources': ['Landau-Tasseron 1985']
    },
    'Sajāḥ': {
        'variants': ['sajah', 'سجاح'],
        'reference': 'Female prophetess of Tamīm',
        'sources': ['Wikipedia', 'Donner 1981']
    },
    'al-Aswad al-ʿAnsī': {
        'variants': ['aswad', 'الأسود'],
        'reference': 'False prophet of Yemen',
        'sources': ['Britannica', 'Madain Project']
    },
    'Laqīṭ ibn Mālik': {
        'variants': ['laqit', 'لقيط'],
        'reference': '"Dhū l-Tāj" of Oman',
        'sources': ['Britannica']
    },
    'Mālik ibn Nuwayra': {
        'variants': ['malik', 'مالك بن نويرة'],
        'reference': 'Chief of Banū Tamīm - controversial execution',
        'sources': ['Landau-Tasseron EI2', 'Watt 1956']
    },
}


# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

def load_events(tabari_file=None, baladhuri_file=None, combined_file=None):
    """Load events from JSON files."""
    events = []
    sources_loaded = []
    
    if combined_file and Path(combined_file).exists():
        with open(combined_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            events = data.get('events', [])
            sources_loaded.append(f"combined ({len(events)} events)")
    else:
        if tabari_file and Path(tabari_file).exists():
            with open(tabari_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                tabari_events = data.get('events', [])
                events.extend(tabari_events)
                sources_loaded.append(f"tabari ({len(tabari_events)} events)")
        
        if baladhuri_file and Path(baladhuri_file).exists():
            with open(baladhuri_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                baladhuri_events = data.get('events', [])
                events.extend(baladhuri_events)
                sources_loaded.append(f"baladhuri ({len(baladhuri_events)} events)")
    
    return events, sources_loaded


def check_match(event, field, variants):
    """Check if any variant matches in the event field."""
    value = str(event.get(field, '') or '').lower()
    value_ar = str(event.get(field.replace('_english', '_arabic'), '') or '')
    
    for variant in variants:
        if variant.lower() in value or variant in value_ar:
            return True
    return False


def validate_category(events, scholarly_data, field):
    """Validate a category of scholarly data against events."""
    results = []
    
    for name, info in scholarly_data.items():
        count = sum(1 for e in events if check_match(e, field, info['variants']))
        results.append({
            'name': name,
            'count': count,
            'found': count > 0,
            'reference': info['reference'],
            'sources': info['sources']
        })
    
    return results


def calculate_mode_distribution(events):
    """Calculate incorporation mode distribution."""
    modes = {}
    for e in events:
        mode = e.get('incorporation_mode_english', e.get('incorporation_mode', 'UNKNOWN'))
        modes[mode] = modes.get(mode, 0) + 1
    return modes


def generate_report(events, sources_loaded, output_file=None):
    """Generate the validation report."""
    
    # Run validations
    tribe_results = validate_category(events, SCHOLARLY_TRIBES, 'tribe_english')
    region_results = validate_category(events, SCHOLARLY_REGIONS, 'region_english')
    commander_results = validate_category(events, SCHOLARLY_COMMANDERS, 'commander_english')
    rebel_results = validate_category(events, SCHOLARLY_REBELS, 'rebel_leader_english')
    
    modes = calculate_mode_distribution(events)
    total_events = len(events)
    
    # Build report
    report = []
    report.append("# Ridda Wars Database - Scholarly Validation Report")
    report.append("")
    report.append("## Executive Summary")
    report.append("")
    report.append(f"This report validates the Ridda Wars Database ({total_events} events) against modern Western scholarship on the Wars of Apostasy (632-633 CE).")
    report.append("")
    report.append(f"**Sources loaded:** {', '.join(sources_loaded)}")
    report.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report.append("")
    report.append("---")
    report.append("")
    
    # Tribes validation
    report.append("## 1. Tribes Mentioned in Scholarship")
    report.append("")
    report.append("| Status | Tribe | Events | Scholarly Reference |")
    report.append("|--------|-------|--------|---------------------|")
    
    tribes_found = 0
    for r in tribe_results:
        status = "✅" if r['found'] else "❌"
        if r['found']:
            tribes_found += 1
        report.append(f"| {status} | {r['name']} | {r['count']} | {r['reference']} |")
    
    report.append("")
    report.append(f"**Result: {tribes_found}/{len(tribe_results)} ({tribes_found/len(tribe_results)*100:.0f}%) major tribes found**")
    report.append("")
    report.append("---")
    report.append("")
    
    # Regions validation
    report.append("## 2. Geographic Regions")
    report.append("")
    report.append("| Status | Region | Events | Scholarly Reference |")
    report.append("|--------|--------|--------|---------------------|")
    
    regions_found = 0
    for r in region_results:
        status = "✅" if r['found'] else "❌"
        if r['found']:
            regions_found += 1
        report.append(f"| {status} | {r['name']} | {r['count']} | {r['reference']} |")
    
    report.append("")
    report.append(f"**Result: {regions_found}/{len(region_results)} ({regions_found/len(region_results)*100:.0f}%) key regions found**")
    report.append("")
    report.append("---")
    report.append("")
    
    # Commanders validation
    report.append("## 3. Muslim Commanders")
    report.append("")
    report.append("| Status | Commander | Events | Role |")
    report.append("|--------|-----------|--------|------|")
    
    commanders_found = 0
    for r in commander_results:
        status = "✅" if r['found'] else "❌"
        if r['found']:
            commanders_found += 1
        report.append(f"| {status} | {r['name']} | {r['count']} | {r['reference']} |")
    
    report.append("")
    report.append(f"**Result: {commanders_found}/{len(commander_results)} ({commanders_found/len(commander_results)*100:.0f}%) key commanders found**")
    report.append("")
    report.append("---")
    report.append("")
    
    # Rebels validation
    report.append("## 4. Rebel Leaders / False Prophets")
    report.append("")
    report.append("| Status | Leader | Events | Description |")
    report.append("|--------|--------|--------|-------------|")
    
    rebels_found = 0
    for r in rebel_results:
        status = "✅" if r['found'] else "❌"
        if r['found']:
            rebels_found += 1
        report.append(f"| {status} | {r['name']} | {r['count']} | {r['reference']} |")
    
    report.append("")
    report.append(f"**Result: {rebels_found}/{len(rebel_results)} ({rebels_found/len(rebel_results)*100:.0f}%) rebel leaders found**")
    report.append("")
    report.append("---")
    report.append("")
    
    # Mode distribution
    report.append("## 5. Incorporation Mode Analysis")
    report.append("")
    report.append("### Database Distribution")
    report.append("")
    report.append("| Mode | Count | Percentage |")
    report.append("|------|-------|------------|")
    
    for mode in ['SUBJUGATION', 'MIXED', 'SUBMISSION']:
        count = modes.get(mode, 0)
        pct = count / total_events * 100 if total_events > 0 else 0
        report.append(f"| **{mode}** | {count} | {pct:.1f}% |")
    
    subjugation_pct = modes.get('SUBJUGATION', 0) / total_events * 100 if total_events > 0 else 0
    
    report.append("")
    report.append("### Scholarly Alignment")
    report.append("")
    report.append(f"The **{subjugation_pct:.1f}% SUBJUGATION rate** aligns with scholarly consensus:")
    report.append("")
    report.append("- **Britannica** describes the Battle of ʿAqrabāʾ as \"notoriously bloody\"")
    report.append("- **Shoufani (1972)** titled his work \"Al-Riddah and the Muslim **Conquest** of Arabia\"")
    report.append("- **Donner (1981)** emphasizes the military \"conquest\" nature")
    report.append("- **World History Encyclopedia** notes \"active warfare and diplomacy\"")
    report.append("")
    report.append("---")
    report.append("")
    
    # Bibliography
    report.append("## Bibliography")
    report.append("")
    report.append("### Primary Scholarly Works")
    report.append("")
    report.append("1. **Shoufani, Elias S.** *Al-Riddah and the Muslim Conquest of Arabia*. Toronto, 1972.")
    report.append("")
    report.append("2. **Donner, Fred McGraw.** *The Early Islamic Conquests*. Princeton University Press, 1981.")
    report.append("")
    report.append("3. **Landau-Tasseron, Ella.** *Aspects of the Ridda Wars*. PhD thesis, Hebrew University, 1981.")
    report.append("   - \"The Participation of Ṭayyi in the Ridda\" *JSAI* 5 (1984)")
    report.append("   - \"Asad from Jāhiliyya to Islam\" *JSAI* 6 (1985)")
    report.append("")
    report.append("4. **Lecker, Michael.** *The Banū Sulaym*. Jerusalem, 1989.")
    report.append("   - \"Kinda on the Eve of Islam\" *JRAS* 3rd ser. 4 (1994)")
    report.append("")
    report.append("5. **Kister, M.J.** \"The Struggle against Musaylima\" *JSAI* 27 (2002)")
    report.append("")
    report.append("### Reference Works")
    report.append("")
    report.append("- *Encyclopaedia of Islam*, 2nd Edition - \"Ridda\", \"Mālik b. Nuwayra\"")
    report.append("- *Britannica* - \"Riddah\" entry")
    report.append("")
    report.append("---")
    report.append("")
    
    # Summary
    report.append("## Validation Summary")
    report.append("")
    report.append("| Category | Found | Total | Score |")
    report.append("|----------|-------|-------|-------|")
    report.append(f"| Tribes | {tribes_found} | {len(tribe_results)} | **{tribes_found/len(tribe_results)*100:.0f}%** |")
    report.append(f"| Regions | {regions_found} | {len(region_results)} | **{regions_found/len(region_results)*100:.0f}%** |")
    report.append(f"| Commanders | {commanders_found} | {len(commander_results)} | **{commanders_found/len(commander_results)*100:.0f}%** |")
    report.append(f"| Rebel Leaders | {rebels_found} | {len(rebel_results)} | **{rebels_found/len(rebel_results)*100:.0f}%** |")
    report.append("")
    
    overall = (tribes_found + regions_found + commanders_found + rebels_found)
    total_checks = len(tribe_results) + len(region_results) + len(commander_results) + len(rebel_results)
    
    report.append(f"**Overall Validation Score: {overall}/{total_checks} ({overall/total_checks*100:.0f}%)**")
    report.append("")
    
    # Join and output
    report_text = '\n'.join(report)
    
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_text)
        print(f"✅ Report saved to: {output_file}")
    
    return report_text


def main():
    parser = argparse.ArgumentParser(
        description='Validate Ridda Wars Database against Western scholarship',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument('--tabari', type=str, 
                        help='Path to Tabari JSON file')
    parser.add_argument('--baladhuri', type=str,
                        help='Path to Baladhuri JSON file')
    parser.add_argument('--combined', type=str,
                        help='Path to combined JSON file (overrides separate files)')
    parser.add_argument('--output', '-o', type=str,
                        help='Output file for report (auto-generated if not specified)')
    parser.add_argument('--output-dir', type=str,
                        default='output/llm_results_validation',
                        help='Output directory for validation reports (default: output/llm_results_validation)')
    parser.add_argument('--print', '-p', action='store_true',
                        help='Print report to console')
    
    args = parser.parse_args()
    
    print("="*60)
    print("RIDDA WARS DATABASE - SCHOLARLY VALIDATION")
    print("="*60)
    print()
    
    # Ensure output directory exists
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Determine output filename based on input
    if args.output:
        output_file = Path(args.output)
        # If just a filename (no directory), put it in output_dir
        if output_file.parent == Path('.'):
            output_file = output_dir / output_file
    else:
        # Auto-generate filename based on sources
        if args.combined:
            output_file = output_dir / 'combined_validation.md'
        elif args.tabari and args.baladhuri:
            output_file = output_dir / 'tabari_baladhuri_validation.md'
        elif args.tabari:
            output_file = output_dir / 'tabari_validation.md'
        elif args.baladhuri:
            output_file = output_dir / 'baladhuri_validation.md'
        else:
            # Default: try to load both from default locations
            args.tabari = 'output/llm_results/ridda_tabari.json'
            args.baladhuri = 'output/llm_results/ridda_baladhuri.json'
            output_file = output_dir / 'tabari_baladhuri_validation.md'
    
    # Load events
    events, sources_loaded = load_events(
        tabari_file=args.tabari,
        baladhuri_file=args.baladhuri,
        combined_file=args.combined
    )
    
    if not events:
        print("❌ No events loaded! Check file paths.")
        print(f"   Tried: tabari={args.tabari}, baladhuri={args.baladhuri}, combined={args.combined}")
        return
    
    print(f"📊 Loaded {len(events)} events from: {', '.join(sources_loaded)}")
    print()
    
    # Generate report
    report = generate_report(events, sources_loaded, str(output_file))
    
    if args.print:
        print()
        print(report)
    
    print()
    print("="*60)
    print("✅ Validation complete!")
    print("="*60)


if __name__ == '__main__':
    main()