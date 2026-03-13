#!/usr/bin/env python3
"""
WĀQIDĪ RIDDA SECTION EXTRACTOR
================================
Extracts the Ridda Wars section from al-Wāqidī's Kitāb al-Ridda
(0207Waqidi.Ridda, Shamela 0012222).

Unlike al-Ṭabarī and al-Balādhurī, al-Wāqidī's text is PRIMARILY about
the Ridda Wars — the book title itself is "al-Ridda." Therefore the
extraction logic is simpler: take everything from the header end up to
the Futūḥ al-ʿIrāq appendix (المثنى بن حارثة الشيباني).

Structure of the source:
  - Lines 1–37:   OpenITI metadata header
  - Lines 39–336: Saqīfa / succession crisis (relevant context)
  - Lines 336–712: Ridda Wars — general + Usāma expedition + initial campaigns
  - Lines 712–1136: Ridda in Najd (Ṭulayḥa, Fujāʾa ibn ʿAbd Yālīl)
  - Lines 1136–1833: Mālik ibn Nuwayra + Musaylima / Yamāma campaign
  - Lines 1833–2132: Ridda of Bahrain
  - Lines 2132–2953: Ridda of Ḥaḍramawt (Kinda, al-Ashʿath ibn Qays)
  - Lines 2954–3266: Futūḥ al-ʿIrāq appendix (al-Muthannā ibn Ḥāritha) — EXCLUDED

The Saqīfa section (lines 39–336) is included because it provides
essential context for the Ridda: Abū Bakr's succession, ʿUmar's debate
on zakāt refusal, and the initial tribal defections.

Author: Generated for Ridda Wars Database Project
Source: OpenITI Corpus — 0207Waqidi.Ridda (Shamela0012222)
Editor: Yaḥyā al-Jabbūrī (Dār al-Gharb al-Islāmī, Beirut, 1990)

Usage:
    python split_waqidi.py input_file.mARkdown [output_file.txt]
    python split_waqidi.py data/0207Waqidi_Ridda_Shamela0012222-ara1.mARkdown
"""

import sys
import os
from pathlib import Path


# The Futūḥ appendix begins at a clearly marked section heading.
# Everything before this heading is Ridda Wars content.
FUTUH_START_MARKERS = [
    'نبذة في ذكر المثنى بن حارثة الشيباني',
    'المثنى بن حارثة الشيباني، وهو أول الفتوح بعد قتال أهل الردة',
    'أول الفتوح بعد قتال أهل الردة',
]

# Major internal section headings (for verification and reporting)
RIDDA_SECTION_HEADINGS = [
    ('اضطراب امر الناس عند وفاة النبي', 'Succession crisis after Prophet\'s death'),
    ('أخبار سقيفة بني ساعدة', 'Saqīfa of Banū Sāʿida'),
    ('ذكر أخبار الردة', 'News of the Ridda'),
    ('ذكر خروج أسامة بن زيد', 'Usāma ibn Zayd\'s expedition'),
    ('ذكر فجاءة بن عبد ياليل', 'Fujāʾa ibn ʿAbd Yālīl'),
    ('خبر مالك بن نويرة ومسيلمة الكذاب', 'Mālik ibn Nuwayra & Musaylima'),
    ('ذكر ردة أهل البحرين', 'Ridda of Bahrain'),
    ('ذكر ارتداد أهل حضرموت من كندة وغيرها', 'Ridda of Ḥaḍramawt / Kinda'),
]

# Ridda-specific terms for content verification
RIDDA_TERMS = [
    'ردة', 'الردة', 'ارتد', 'ارتدت', 'المرتدين',
    'مسيلمة', 'طليحة', 'سجاح', 'الأسود العنسي',
    'خالد بن الوليد', 'أبو بكر', 'اليمامة', 'بزاخة',
    'منع الزكاة', 'منعوا الزكاة',
    'بنو حنيفة', 'بنو أسد', 'بنو تميم',
    'البحرين', 'حضرموت', 'كندة',
]


def find_futuh_boundary(lines: list) -> int:
    """Find the line where the Futūḥ al-ʿIrāq appendix begins."""
    
    for i, line in enumerate(lines):
        for marker in FUTUH_START_MARKERS:
            if marker in line:
                print(f"  Found Futūḥ boundary at line {i+1}: {line.strip()[:80]}...")
                return i
    
    # If not found, return total lines (use everything)
    print("  ⚠ No Futūḥ boundary found — using entire file")
    return len(lines)


def find_header_end(lines: list) -> int:
    """Find the end of the OpenITI metadata header."""
    
    for i, line in enumerate(lines):
        if '#META#Header#End#' in line:
            print(f"  Header ends at line {i+1}")
            return i + 1
    
    # Fallback: skip lines starting with #META#
    for i, line in enumerate(lines):
        if not line.startswith('#META#') and not line.startswith('######OpenITI'):
            if line.strip():
                print(f"  No explicit header end marker, content starts at line {i+1}")
                return i
    
    return 0


def find_internal_sections(lines: list, start: int, end: int) -> list:
    """Identify major named sections within the Ridda content."""
    
    sections = []
    for i in range(start, end):
        line = lines[i]
        if '### |' in line and len(line.strip()) > 6:
            # Clean the heading text
            heading = line.replace('### |', '').strip()
            heading = heading.replace('[', '').replace(']', '')
            
            # Match against known headings
            matched = None
            for ar_heading, en_heading in RIDDA_SECTION_HEADINGS:
                if ar_heading in heading:
                    matched = en_heading
                    break
            
            sections.append({
                'line': i + 1,
                'heading_ar': heading[:80],
                'heading_en': matched or '(unnamed subsection)',
                'is_named': matched is not None,
            })
    
    return sections


def extract_ridda_section(input_path: str, output_path: str = None) -> str:
    """Extract the Ridda Wars section from al-Wāqidī's Kitāb al-Ridda."""
    
    input_path = Path(input_path)
    
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)
    
    # Default output path
    if output_path is None:
        output_path = input_path.parent / "waqidi_ridda_section.txt"
    else:
        output_path = Path(output_path)
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*60}")
    print("WĀQIDĪ RIDDA WARS SECTION EXTRACTOR")
    print(f"{'='*60}")
    print(f"Input:  {input_path}")
    print(f"Output: {output_path}")
    
    # Read the file
    print(f"\n📖 Loading source file...")
    with open(input_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    lines = content.split('\n')
    total_lines = len(lines)
    total_size = len(content)
    
    print(f"  Total lines: {total_lines:,}")
    print(f"  Total size: {total_size:,} chars ({total_size/1024:.1f} KB)")
    
    # Find boundaries
    print(f"\n🔍 Identifying section boundaries...")
    header_end = find_header_end(lines)
    futuh_start = find_futuh_boundary(lines)
    
    # The Ridda section: from header_end to futuh_start
    ridda_lines = lines[header_end:futuh_start]
    ridda_content = '\n'.join(ridda_lines)
    
    extracted_lines = len(ridda_lines)
    extracted_size = len(ridda_content)
    
    print(f"\n📊 Extraction results:")
    print(f"  Header end:   line {header_end + 1}")
    print(f"  Futūḥ start:  line {futuh_start + 1}")
    print(f"  Extracted:    {extracted_lines:,} lines ({extracted_lines/total_lines*100:.1f}%)")
    print(f"  Size:         {extracted_size:,} chars ({extracted_size/1024:.1f} KB)")
    print(f"  Excluded:     {total_lines - extracted_lines:,} lines (header + Futūḥ appendix)")
    
    # Find and report internal sections
    print(f"\n📑 Internal sections:")
    sections = find_internal_sections(lines, header_end, futuh_start)
    named_sections = [s for s in sections if s['is_named']]
    unnamed_sections = [s for s in sections if not s['is_named']]
    
    for s in named_sections:
        print(f"  ✓ Line {s['line']:>5}: {s['heading_en']}")
        print(f"              AR: {s['heading_ar']}")
    print(f"  + {len(unnamed_sections)} unnamed subsections (### ||)")
    
    # Build header
    section_listing = '\n'.join(
        f"#   Line {s['line']:>5}: {s['heading_en']}" 
        for s in named_sections
    )
    
    header = f"""# ==============================================================================
# RIDDA WARS SECTION — EXTRACTED FROM AL-WĀQIDĪ'S KITĀB AL-RIDDA
# ==============================================================================
# Source:  {input_path.name}
# Author:  Muḥammad ibn ʿUmar al-Wāqidī (d. 207/823)
# Title:   Kitāb al-Ridda (كتاب الردة)
# Editor:  Yaḥyā al-Jabbūrī (Dār al-Gharb al-Islāmī, Beirut, 1990)
# Period:  11–12 AH / 632–633 CE (Caliphate of Abū Bakr)
# Lines:   {header_end + 1} to {futuh_start} (of {total_lines} total)
# Size:    {extracted_size:,} chars ({extracted_lines:,} lines)
# Excluded: Futūḥ al-ʿIrāq appendix (al-Muthannā ibn Ḥāritha al-Shaybānī)
#
# Major sections:
{section_listing}
# ==============================================================================

"""
    
    # Write output
    print(f"\n💾 Writing output file...")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(header)
        f.write(ridda_content)
    
    print(f"  ✅ Saved: {output_path}")
    
    # Verify content
    print(f"\n🔎 Verifying Ridda content...")
    for term in RIDDA_TERMS:
        count = ridda_content.count(term)
        status = '✓' if count > 0 else '⚠'
        print(f"  {status} '{term}': {count}")
    
    # Estimate chunks for pipeline
    chunk_size = 20000
    overlap = 2000
    pos = 0
    n_chunks = 0
    while pos < extracted_size:
        end = min(pos + chunk_size, extracted_size)
        n_chunks += 1
        pos = end - overlap if end < extracted_size else end
    
    print(f"\n📦 Estimated pipeline chunks: ~{n_chunks}")
    print(f"   (Ṭabarī: ~35 chunks, Balādhurī: ~8 chunks)")
    
    # Summary statistics
    print(f"\n📊 Source comparison:")
    print(f"   al-Wāqidī (d. 207): {extracted_size:>8,} chars  — dedicated Ridda monograph")
    print(f"   al-Balādhurī (d. 279): extracted section from Futūḥ al-Buldān")
    print(f"   al-Ṭabarī (d. 310):    extracted section from Tārīkh")
    print(f"\n   al-Wāqidī is the EARLIEST of the three sources.")
    print(f"   His Ridda is a dedicated monograph, not an excerpt from a larger work.")
    print(f"   NOTE: Wāqidī transmits via Ibn al-Aʿtham al-Kūfī (Ibn Aʿtham redaction).")
    
    return str(output_path)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    extract_ridda_section(input_file, output_file)
    
    print(f"\n{'='*60}")
    print("Done! Use the extracted file with the pipeline:")
    print("  python ridda_pipeline.py --source waqidi --data-dir data")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
