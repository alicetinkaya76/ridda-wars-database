#!/usr/bin/env python3
"""
RIDDA WARS PIPELINE - TEST SCRIPT
==================================
Tests the pipeline with sample Arabic text without needing full OpenITI files.

Usage:
    python test_ridda_pipeline.py
    python test_ridda_pipeline.py --with-api  # Test with actual API call
"""

import os
import sys
import json
import argparse
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

# =============================================================================
# SAMPLE RIDDA WARS TEXT (for testing)
# =============================================================================

SAMPLE_RIDDA_TEXT = """
ذكر ردة أهل اليمامة وقتال مسيلمة الكذاب

لما توفي رسول الله صلى الله عليه وسلم ارتد كثير من العرب عن الإسلام ومنعوا الزكاة.
وكان مسيلمة بن حبيب الحنفي قد ادعى النبوة في حياة رسول الله صلى الله عليه وسلم في اليمامة.
فلما مات النبي صلى الله عليه وسلم اشتد أمر مسيلمة وتبعه بنو حنيفة.

فبعث أبو بكر الصديق رضي الله عنه خالد بن الوليد إلى اليمامة لقتال مسيلمة وبني حنيفة.
فسار خالد بجيش المسلمين حتى وصل إلى اليمامة.
فالتقى الجمعان بعقرباء وكانت وقعة عظيمة قتل فيها خلق كثير من الطرفين.
وقاتل المسلمون قتالا شديدا حتى هزموا المرتدين وقتل مسيلمة الكذاب.
قتله وحشي بن حرب برمحه. وكان ذلك في سنة اثنتي عشرة من الهجرة.

وأما بنو سليم فإنهم لم يقاتلوا وأسلموا ورجعوا إلى الطاعة وأدوا الزكاة.

وبعث أبو بكر العلاء بن الحضرمي إلى البحرين لقتال المرتدين هناك.
وكان أهل البحرين قد ارتدوا إلا عبد القيس فإنهم ثبتوا على الإسلام تحت قيادة الجارود بن بشر.
فقاتل العلاء المرتدين حتى ظفر بهم.

وسار عكرمة بن أبي جهل إلى عمان لقتال لقيط بن مالك ذي التاج الذي ارتد بأهل عمان.
فحاربه المسلمون في دبا وانتصروا عليه وقتلوه.
"""

# =============================================================================
# TEST FUNCTIONS
# =============================================================================

def test_imports():
    """Test that all imports work correctly."""
    print("=" * 60)
    print("TEST 1: Imports")
    print("=" * 60)
    
    try:
        import yaml
        print("  ✅ PyYAML imported successfully")
    except ImportError:
        print("  ❌ PyYAML not available - run: pip install pyyaml")
        return False
    
    try:
        import anthropic
        print("  ✅ Anthropic imported successfully")
    except ImportError:
        print("  ❌ Anthropic not available - run: pip install anthropic")
        return False
    
    try:
        from ridda_pipeline import (
            normalize_arabic,
            detect_incorporation_terms,
            classify_incorporation_mode,
            transliterate_brill,
            detect_year_in_text,
            RIDDA_SOURCES,
            RIDDA_TRIBES,
            RIDDA_BATTLE_SITES,
            RIDDA_COMMANDERS,
            RIDDA_LEADERS
        )
        print("  ✅ ridda_pipeline modules imported successfully")
    except ImportError as e:
        print(f"  ❌ Failed to import ridda_pipeline: {e}")
        return False
    
    return True


def test_arabic_processing():
    """Test Arabic text processing functions."""
    print("\n" + "=" * 60)
    print("TEST 2: Arabic Text Processing")
    print("=" * 60)
    
    from ridda_pipeline import normalize_arabic, detect_incorporation_terms, classify_incorporation_mode
    
    # Test normalization
    test_text = "مُسَيْلِمَة الكَذَّاب"
    normalized = normalize_arabic(test_text)
    print(f"  Original:   {test_text}")
    print(f"  Normalized: {normalized}")
    
    # Test term detection on sample text
    terms = detect_incorporation_terms(SAMPLE_RIDDA_TEXT)
    
    print(f"\n  Detected qitāl (SUBJUGATION) terms: {len(terms['qital_terms'])}")
    for term in terms['qital_terms'][:5]:
        print(f"    - {term}")
    
    print(f"\n  Detected ṭāʿa (SUBMISSION) terms: {len(terms['taa_terms'])}")
    for term in terms['taa_terms'][:5]:
        print(f"    - {term}")
    
    print(f"\n  Detected Ridda terms: {len(terms['ridda_terms'])}")
    for term in terms['ridda_terms'][:5]:
        print(f"    - {term}")
    
    # Test classification
    mode = classify_incorporation_mode(terms)
    print(f"\n  Classification result: {mode}")
    
    return True


def test_year_detection():
    """Test year detection in Arabic text."""
    print("\n" + "=" * 60)
    print("TEST 3: Year Detection")
    print("=" * 60)
    
    from ridda_pipeline import detect_year_in_text
    
    test_cases = [
        ("وكان ذلك في سنة اثنتي عشرة من الهجرة", 12),
        ("سنة 11 هجرية", 11),
        ("في سنة إحدى عشرة", 11),
        ("سنة 12", 12),
        ("لا يوجد تاريخ", None),
    ]
    
    all_passed = True
    for text, expected in test_cases:
        result = detect_year_in_text(text)
        status = "✅" if result == expected else "❌"
        if result != expected:
            all_passed = False
        print(f"  {status} '{text[:40]}...' → {result} (expected: {expected})")
    
    return all_passed


def test_transliteration():
    """Test Brill EI3 transliteration."""
    print("\n" + "=" * 60)
    print("TEST 4: Brill EI3 Transliteration")
    print("=" * 60)
    
    from ridda_pipeline import transliterate_brill
    
    test_cases = [
        ("مسيلمة", "Musaylima"),
        ("خالد بن الوليد", "Khālid ibn al-Walīd"),
        ("بنو حنيفة", "Banū Ḥanīfa"),
        ("اليمامة", "al-Yamāma"),
        ("عقرباء", "ʿAqrabāʾ"),
        ("بزاخة", "Buzākha"),
    ]
    
    all_passed = True
    for arabic, expected in test_cases:
        result = transliterate_brill(arabic)
        status = "✅" if result == expected else "❌"
        if result != expected:
            all_passed = False
        print(f"  {status} {arabic} → {result} (expected: {expected})")
    
    return all_passed


def test_data_structures():
    """Test that data structures are properly defined."""
    print("\n" + "=" * 60)
    print("TEST 5: Data Structures")
    print("=" * 60)
    
    from ridda_pipeline import (
        RIDDA_SOURCES,
        RIDDA_TRIBES,
        RIDDA_BATTLE_SITES,
        RIDDA_COMMANDERS,
        RIDDA_LEADERS,
        RIDDA_REGIONS
    )
    
    print(f"  Sources:      {len(RIDDA_SOURCES)} defined")
    for name, info in RIDDA_SOURCES.items():
        print(f"    - {name}: {info['name']} (d. {info['death_year']})")
    
    print(f"\n  Tribes:       {len(RIDDA_TRIBES)} defined")
    for name in list(RIDDA_TRIBES.keys())[:5]:
        info = RIDDA_TRIBES[name]
        print(f"    - {name}: {info['name']} ({info['region']})")
    
    print(f"\n  Battle sites: {len(RIDDA_BATTLE_SITES)} defined")
    for name, info in RIDDA_BATTLE_SITES.items():
        print(f"    - {name}: {info['name']} (lat: {info['lat']}, lon: {info['lon']})")
    
    print(f"\n  Commanders:   {len(RIDDA_COMMANDERS)} defined")
    for name in list(RIDDA_COMMANDERS.keys())[:3]:
        info = RIDDA_COMMANDERS[name]
        print(f"    - {name}: {info['name']}")
    
    print(f"\n  Rebel leaders: {len(RIDDA_LEADERS)} defined")
    for name in list(RIDDA_LEADERS.keys())[:3]:
        info = RIDDA_LEADERS[name]
        print(f"    - {name}: {info['name']} ({info['tribe']})")
    
    print(f"\n  Regions:      {len(RIDDA_REGIONS)} defined")
    for name, info in RIDDA_REGIONS.items():
        print(f"    - {name}: {info['name']} (lat: {info['center_lat']}, lon: {info['center_lon']})")
    
    return True


def test_config_loading():
    """Test configuration file loading."""
    print("\n" + "=" * 60)
    print("TEST 6: Configuration Loading")
    print("=" * 60)
    
    from ridda_pipeline import ConfigLoader
    
    config_dir = Path(__file__).parent
    loader = ConfigLoader(config_dir)
    
    # Test API config
    api_config = loader.get_api_config()
    print(f"  API config loaded: {bool(api_config)}")
    print(f"    Model: {api_config.get('model', 'not set')}")
    
    # Test extraction config
    ext_config = loader.get_extraction_config()
    print(f"\n  Extraction config:")
    print(f"    min_year: {ext_config.get('min_year')}")
    print(f"    max_year: {ext_config.get('max_year')}")
    print(f"    chunk_size: {ext_config.get('chunk_size')}")
    
    # Test source config
    for source in ['tabari', 'baladhuri']:
        src_config = loader.get_source_config(source)
        print(f"\n  Source '{source}':")
        print(f"    name: {src_config.get('name')}")
        print(f"    structure: {src_config.get('structure')}")
    
    return True


def test_api_extraction(api_key: str = None):
    """Test actual API extraction with sample text."""
    print("\n" + "=" * 60)
    print("TEST 7: API Extraction (Live)")
    print("=" * 60)
    
    # Try to get API key from various sources
    if not api_key:
        api_key = os.environ.get('ANTHROPIC_API_KEY')
    
    # Try to load from config file
    if not api_key:
        from ridda_pipeline import ConfigLoader
        config_dir = Path(__file__).parent
        config_loader = ConfigLoader(config_dir)
        api_config = config_loader.get_api_config()
        api_key = api_config.get('api_key')
    
    if not api_key:
        print("  ⚠️  No API key provided. Skipping live API test.")
        print("     Set ANTHROPIC_API_KEY or use --api-key argument")
        print("     Or add api_key to ridda_config.yaml")
        return None
    
    import anthropic
    from ridda_pipeline import ConfigLoader, extract_with_claude
    
    client = anthropic.Anthropic(api_key=api_key)
    config_dir = Path(__file__).parent
    config_loader = ConfigLoader(config_dir)
    extraction_config = config_loader.get_extraction_config()
    
    print("  Sending sample text to Claude API...")
    
    events, skipped = extract_with_claude(
        client, config_loader, SAMPLE_RIDDA_TEXT,
        'tabari', 12, extraction_config
    )
    
    print(f"\n  Results:")
    print(f"    Events extracted: {len(events)}")
    print(f"    Skipped: {len(skipped)}")
    
    if events:
        print(f"\n  Sample event (bilingual fields):")
        event = events[0]
        # Show bilingual pairs
        bilingual_pairs = [
            ('tribe_arabic', 'tribe_english'),
            ('region_arabic', 'region_english'),
            ('incorporation_mode_arabic', 'incorporation_mode_english'),
            ('commander_arabic', 'commander_english'),
            ('rebel_leader_arabic', 'rebel_leader_english'),
            ('battle_site_arabic', 'battle_site_english'),
            ('outcome_arabic', 'outcome_english'),
        ]
        for ar_key, en_key in bilingual_pairs:
            ar_val = event.get(ar_key, event.get(ar_key.replace('_arabic', ''), ''))
            en_val = event.get(en_key, event.get(en_key.replace('_english', ''), ''))
            if ar_val or en_val:
                field_name = ar_key.replace('_arabic', '')
                print(f"    {field_name}:")
                print(f"      AR: {ar_val}")
                print(f"      EN: {en_val}")
        
        # Show year
        print(f"    year_ah: {event.get('year_ah', '')}")
        print(f"    year_ce: {event.get('year_ce', '')}")
        print(f"    confidence: {event.get('confidence', '')}")
    
    return len(events) > 0


def create_sample_data_file():
    """Create a sample data file for testing."""
    print("\n" + "=" * 60)
    print("Creating Sample Data File")
    print("=" * 60)
    
    data_dir = Path(__file__).parent / 'data'
    data_dir.mkdir(exist_ok=True)
    
    sample_file = data_dir / 'sample_ridda_text.txt'
    with open(sample_file, 'w', encoding='utf-8') as f:
        f.write(SAMPLE_RIDDA_TEXT)
    
    print(f"  ✅ Created: {sample_file}")
    print(f"  Size: {len(SAMPLE_RIDDA_TEXT)} characters")
    
    return str(sample_file)


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Test the Ridda Wars Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--with-api', action='store_true',
                        help='Run live API test')
    parser.add_argument('--api-key', type=str,
                        help='Anthropic API key (or set ANTHROPIC_API_KEY)')
    parser.add_argument('--create-sample', action='store_true',
                        help='Create sample data file')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("RIDDA WARS PIPELINE - TEST SUITE")
    print("=" * 60)
    
    results = {}
    
    # Run tests
    results['imports'] = test_imports()
    
    if results['imports']:
        results['arabic'] = test_arabic_processing()
        results['year'] = test_year_detection()
        results['transliteration'] = test_transliteration()
        results['data'] = test_data_structures()
        results['config'] = test_config_loading()
        
        if args.with_api:
            results['api'] = test_api_extraction(args.api_key)
        
        if args.create_sample:
            create_sample_data_file()
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    for test_name, passed in results.items():
        if passed is None:
            status = "⏭️  SKIPPED"
        elif passed:
            status = "✅ PASSED"
        else:
            status = "❌ FAILED"
        print(f"  {test_name:20} {status}")
    
    # Overall
    failed = sum(1 for r in results.values() if r is False)
    if failed == 0:
        print("\n✅ All tests passed!")
    else:
        print(f"\n❌ {failed} test(s) failed")
    
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())