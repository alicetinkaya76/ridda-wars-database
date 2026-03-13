#!/usr/bin/env python3
"""
TABARI VOLUME SPLITTER FOR RIDDA WARS
======================================
Extracts only the relevant portions of al-Ṭabarī's Tārīkh that contain
Ridda Wars content (11-12 AH / 632-633 CE).

The full Tabari file is ~190,000 lines and 8.9MB. This script extracts:
- Year 11 AH (حوادث السنة الحادية عشرة بعد وفاة رسول الله)
- Year 12 AH (until Abu Bakr's death / Umar's succession)

This reduces processing from 488 chunks to ~35-40 chunks, saving significant
API costs and time.

Usage:
    python split_tabari.py input_file.txt [output_file.txt]
    python split_tabari.py data/0310Tabari_Tarikh.Masaha001963Vols-ara1
"""

import sys
import os
import re
from pathlib import Path


# Markers for Ridda Wars section - must match exactly
RIDDA_START_MARKERS = [
    '[حوادث السنة الحادية العشرة بعد وفاة رسول الله]',
    'حوادث السنة الحادية العشرة بعد وفاة رسول الله',
    'حوادث السنة الحادية عشره',
    'السنة الحادية عشرة بعد',
]

# Backup start markers - search near Prophet's death
RIDDA_BACKUP_START = [
    'ذكر الخبر عن بدء مرض رسول الله',
    'بدء مرض رسول الله',
    'وفاة رسول الله',
]

RIDDA_END_MARKERS = [
    'ذكر استخلافه عمر بن الخطاب',
    'ذكر استخلاف عمر',
    'خلافة عمر بن الخطاب',
    'خلافه عمر بن الخطاب',
]

# Additional content markers for verification
RIDDA_CONTENT_MARKERS = [
    'مسيلمة',
    'طليحة',
    'سجاح',
    'الأسود العنسي',
    'حروب الردة',
    'الردة',
    'بنو حنيفة',
    'بزاخة',
    'عقرباء',
    'اليمامة',
]


def find_ridda_section(lines: list) -> tuple:
    """Find the start and end line numbers of the Ridda Wars section."""
    
    start_line = None
    end_line = None
    
    # Find start marker - primary markers
    for i, line in enumerate(lines):
        for marker in RIDDA_START_MARKERS:
            if marker in line:
                start_line = i
                print(f"  Found start marker at line {i+1}: '{marker[:60]}...'")
                break
        if start_line:
            break
    
    # If no primary start marker, try backup markers
    if start_line is None:
        print("  No primary start marker found, trying backup markers...")
        for i, line in enumerate(lines):
            for marker in RIDDA_BACKUP_START:
                if marker in line:
                    # Found Prophet's death section, Ridda starts shortly after
                    start_line = i
                    print(f"  Found backup marker at line {i+1}: '{marker[:60]}...'")
                    break
            if start_line:
                break
    
    # If still not found, search for dense Ridda content (multiple markers close together)
    if start_line is None:
        print("  No markers found, searching for Ridda content density...")
        window_size = 100
        min_markers = 3
        
        for i in range(0, len(lines) - window_size, 50):
            window = '\n'.join(lines[i:i+window_size])
            marker_count = sum(1 for marker in RIDDA_CONTENT_MARKERS if marker in window)
            if marker_count >= min_markers:
                start_line = i
                print(f"  Found Ridda content cluster at line {i+1} ({marker_count} markers)")
                break
    
    # Find end marker (after start)
    if start_line:
        for i, line in enumerate(lines[start_line:], start=start_line):
            for marker in RIDDA_END_MARKERS:
                if marker in line:
                    # Include some extra lines after end marker for context
                    end_line = min(len(lines), i + 500)
                    print(f"  Found end marker at line {i+1}: '{marker[:60]}...'")
                    break
            if end_line:
                break
    
    # If no end marker, use a reasonable default
    if start_line and end_line is None:
        end_line = min(len(lines), start_line + 7000)
        print(f"  No end marker found, using default end at line {end_line}")
    
    return start_line, end_line


def extract_ridda_section(input_path: str, output_path: str = None) -> str:
    """Extract the Ridda Wars section from Tabari."""
    
    input_path = Path(input_path)
    
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)
    
    # Default output path
    if output_path is None:
        output_path = input_path.parent / f"{input_path.stem}_ridda_section.txt"
    else:
        output_path = Path(output_path)
    
    print(f"\n{'='*60}")
    print("TABARI RIDDA WARS SECTION EXTRACTOR")
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
    print(f"  Total size: {total_size:,} chars ({total_size/1024/1024:.2f} MB)")
    
    # Find Ridda section
    print(f"\n🔍 Searching for Ridda Wars section...")
    start_line, end_line = find_ridda_section(lines)
    
    if start_line is None:
        print("Error: Could not find Ridda Wars section in the file.")
        sys.exit(1)
    
    # Extract section
    ridda_lines = lines[start_line:end_line]
    ridda_content = '\n'.join(ridda_lines)
    
    extracted_lines = len(ridda_lines)
    extracted_size = len(ridda_content)
    
    print(f"\n📊 Extraction results:")
    print(f"  Start line: {start_line + 1:,}")
    print(f"  End line:   {end_line:,}")
    print(f"  Extracted:  {extracted_lines:,} lines ({extracted_lines/total_lines*100:.1f}%)")
    print(f"  Size:       {extracted_size:,} chars ({extracted_size/1024/1024:.2f} MB)")
    print(f"  Reduction:  {(1 - extracted_size/total_size)*100:.1f}%")
    
    # Add header
    header = f"""# ==============================================================================
# RIDDA WARS SECTION - EXTRACTED FROM AL-ṬABARĪ'S TĀRĪKH
# ==============================================================================
# Source: {input_path.name}
# Lines: {start_line + 1} to {end_line}
# Period: 11-12 AH / 632-633 CE (Caliphate of Abū Bakr)
# Content: Ridda Wars (حروب الردة) - Wars of Apostasy
# ==============================================================================

"""
    
    # Write output
    print(f"\n💾 Writing output file...")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(header)
        f.write(ridda_content)
    
    print(f"  ✅ Saved: {output_path}")
    
    # Verify content
    print(f"\n🔎 Verifying content...")
    ridda_terms = ['ردة', 'مسيلمة', 'طليحة', 'خالد بن الوليد', 'أبي بكر', 'اليمامة']
    for term in ridda_terms:
        count = ridda_content.count(term)
        if count > 0:
            print(f"  ✓ '{term}': {count} occurrences")
        else:
            print(f"  ⚠ '{term}': not found")
    
    # Estimate chunks
    chunk_size = 20000
    estimated_chunks = (extracted_size // chunk_size) + 1
    print(f"\n📦 Estimated chunks for pipeline: ~{estimated_chunks}")
    print(f"   (Original would be ~{(total_size // chunk_size) + 1} chunks)")
    
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
    print("  python ridda_pipeline.py --source tabari --data-dir .")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()