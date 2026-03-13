#!/usr/bin/env python3
"""
BALADHURI VOLUME SPLITTER FOR RIDDA WARS
=========================================
Extracts only the relevant portions of al-Balādhurī's Futūḥ al-Buldān 
that contain Ridda Wars content (11-12 AH / 632-633 CE).

The Futūḥ al-Buldān is organized geographically, so Ridda content is in:
- فتح عمان (Conquest of Oman)
- غزوة البحرين (Bahrain Campaign)
- دعوة النبي أهل اليمامة (Prophet's Call to al-Yamāma)
- خبر ردة العرب (News of Arab Apostasy)
- ردة بني وليعة (Apostasy of Banū Walīʿa)
- أمر الأسود العنسي (Matter of al-Aswad al-ʿAnsī)

NOTE: This file may have duplicate sections. This script extracts ALL 
relevant content from multiple locations.

Usage:
    python split_baladhuri.py input_file.txt [output_file.txt]
    python split_baladhuri.py data/0279Baladhuri.FutuhBuldan.Masaha002329Vols-ara1
"""

import sys
import os
from pathlib import Path


# Key Ridda content markers - extract paragraphs containing these
RIDDA_KEYWORDS = [
    # Ridda terms
    'ردة', 'ارتد', 'ارتدت', 'المرتدين',
    # False prophets
    'مسيلمة', 'طليحة', 'سجاح', 'الأسود العنسي',
    # Key commanders
    'خالد بن الوليد',
    # Regions during Ridda
    'اليمامة', 'البحرين', 'عمان',
    # Key events
    'حروب الردة', 'منعوا الزكاة',
    # Tribes
    'بنو حنيفة', 'بنو أسد', 'بنو تميم',
]

# Section headers that indicate Ridda content
RIDDA_SECTION_HEADERS = [
    'فتح عمان',
    'غزوة البحرين',
    'دعوة النبي',
    'اليمامة',
    'خبر ردة العرب',
    'ردة بني',
    'أمر الأسود العنسي',
]

# Markers that indicate END of Ridda-relevant content
END_MARKERS = [
    'فتوح الشأم',
    'فتوح الشام', 
    'فتح دمشق',
    'فتوح العراق',
]


def extract_ridda_sections(lines: list) -> list:
    """Extract all Ridda-relevant sections from the text."""
    
    extracted_ranges = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # Check if this is a Ridda section header
        is_ridda_header = any(header in line for header in RIDDA_SECTION_HEADERS)
        
        if is_ridda_header and '### |' in line:
            # Found a Ridda section, extract until next major section or end marker
            start = i
            end = i + 1
            
            while end < len(lines):
                next_line = lines[end]
                
                # Check for end markers
                if any(marker in next_line for marker in END_MARKERS):
                    break
                
                # Check for new major section (not Ridda-related)
                if '### |' in next_line and not any(h in next_line for h in RIDDA_SECTION_HEADERS):
                    # Check if this section has Ridda content
                    has_ridda = False
                    for j in range(end, min(end + 50, len(lines))):
                        if any(kw in lines[j] for kw in RIDDA_KEYWORDS[:10]):
                            has_ridda = True
                            break
                    if not has_ridda:
                        break
                
                end += 1
            
            extracted_ranges.append((start, end))
            print(f"  Found section at lines {start+1}-{end}: {line[:60].strip()}...")
            i = end
        else:
            i += 1
    
    return extracted_ranges


def merge_ranges(ranges: list) -> list:
    """Merge overlapping or adjacent ranges."""
    if not ranges:
        return []
    
    sorted_ranges = sorted(ranges)
    merged = [sorted_ranges[0]]
    
    for start, end in sorted_ranges[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end + 10:  # Allow small gap
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    
    return merged


def extract_ridda_content(input_path: str, output_path: str = None) -> str:
    """Extract all Ridda Wars content from Baladhuri."""
    
    input_path = Path(input_path)
    
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)
    
    # Default output path
    if output_path is None:
        output_path = input_path.parent / f"{input_path.stem}_ridda_section.txt"
    else:
        output_path = Path(output_path)
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*60}")
    print("BALADHURI RIDDA WARS SECTION EXTRACTOR")
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
    
    # Find all Ridda sections
    print(f"\n🔍 Searching for Ridda Wars sections...")
    ranges = extract_ridda_sections(lines)
    
    if not ranges:
        print("Error: Could not find Ridda Wars sections in the file.")
        sys.exit(1)
    
    # Merge overlapping ranges
    merged_ranges = merge_ranges(ranges)
    print(f"\n📊 Found {len(merged_ranges)} distinct sections")
    
    # Extract all content
    extracted_lines = []
    for start, end in merged_ranges:
        extracted_lines.extend(lines[start:end])
        extracted_lines.append('')  # Separator
    
    ridda_content = '\n'.join(extracted_lines)
    extracted_count = len(extracted_lines)
    extracted_size = len(ridda_content)
    
    print(f"\n📊 Extraction results:")
    print(f"  Sections: {len(merged_ranges)}")
    print(f"  Extracted: {extracted_count:,} lines ({extracted_count/total_lines*100:.1f}%)")
    print(f"  Size: {extracted_size:,} chars ({extracted_size/1024/1024:.2f} MB)")
    print(f"  Reduction: {(1 - extracted_size/total_size)*100:.1f}%")
    
    # Add header
    section_list = '\n'.join([f"#   - Lines {s+1}-{e}" for s, e in merged_ranges])
    header = f"""# ==============================================================================
# RIDDA WARS CONTENT - EXTRACTED FROM AL-BALĀDHURĪ'S FUTŪḤ AL-BULDĀN
# ==============================================================================
# Source: {input_path.name}
# Period: 11-12 AH / 632-633 CE (Caliphate of Abū Bakr)
# Content: Ridda Wars (حروب الردة) - Wars of Apostasy
# Sections extracted:
{section_list}
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
    ridda_terms = ['ردة', 'مسيلمة', 'طليحة', 'خالد بن الوليد', 'أبي بكر', 'اليمامة', 'البحرين', 'عمان']
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
    
    extract_ridda_content(input_file, output_file)
    
    print(f"\n{'='*60}")
    print("Done! Use the extracted file with the pipeline:")
    print("  python ridda_pipeline.py --source baladhuri --data-dir data")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()