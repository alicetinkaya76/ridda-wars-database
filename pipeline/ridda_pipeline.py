#!/usr/bin/env python3
"""
RIDDA WARS DATABASE PIPELINE v1.0
==================================

A Computational Analysis of Tribal Unification in Early Islam using Large Language Models

Based on the EICD Pipeline v4.2, adapted for:
- Ridda Wars period: 11-12 AH / 632-633 CE
- Tribal interactions instead of city conquests
- Arabian Peninsula geography (Najd, Yemen, Oman, al-Bahrayn)
- Classification: Military Subjugation (qitāl) vs Voluntary Submission (ṭāʿa)

SOURCES:
- al-Ṭabarī (d. 310) - Tārīkh al-Rusul wa-l-Mulūk
- al-Balādhurī (d. 279) - Futūḥ al-Buldān

TERMINOLOGY (Brill EI3):
  SUBJUGATION (qitāl) - Military campaign against rebellious tribe
  SUBMISSION (ṭāʿa) - Voluntary return to Islamic authority
  
OUTPUT:
  Structured database of tribal interactions with geocoded locations
  using al-Turayyā (Thuriyya) Gazetteer coordinates

Usage:
    python ridda_pipeline.py --source tabari
    python ridda_pipeline.py --source baladhuri
    python ridda_pipeline.py --source all
    python ridda_pipeline.py --source tabari --max-chunks 50

Author: Adapted from EICD Project for Ridda Wars Database
Version: 1.0
Date: January 2025
"""

import os
import sys
import json
import csv
import re
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Set, Any
from collections import Counter, defaultdict
from dataclasses import dataclass, field

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# OPTIONAL IMPORTS
# =============================================================================

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    logger.warning("PyYAML not available: pip install pyyaml")

try:
    import anthropic
    CLAUDE_AVAILABLE = True
except ImportError:
    CLAUDE_AVAILABLE = False
    logger.warning("Anthropic not available: pip install anthropic")

# =============================================================================
# RIDDA WARS TERMINOLOGY (Brill EI3)
# =============================================================================

INCORPORATION_MODES = {
    'SUBJUGATION': 'qitāl - Military subjugation through armed campaign',
    'SUBMISSION': 'ṭāʿa - Voluntary submission/return to Islamic authority',
    'MIXED': 'Combined elements of both military and diplomatic resolution',
    'UNKNOWN': 'Cannot determine from available evidence'
}

# =============================================================================
# RIDDA WARS SOURCES
# =============================================================================

RIDDA_SOURCES = {
    'tabari': {
        'name': 'al-Ṭabarī',
        'name_ar': 'الطبري',
        'full_title': 'Tārīkh al-Rusul wa-l-Mulūk',
        'full_title_ar': 'تاريخ الرسل والملوك',
        'death_year': 310,
        'structure': 'annalistic',
        'openiti_file': '0310Tabari.Tarikh.Masaha001963Vols-ara1',
        'year_range': (11, 12),  # Ridda Wars specific
        'notes': 'Primary annalistic source for Ridda Wars campaigns'
    },
    'baladhuri': {
        'name': 'al-Balādhurī',
        'name_ar': 'البلاذري',
        'full_title': 'Futūḥ al-Buldān',
        'full_title_ar': 'فتوح البلدان',
        'death_year': 279,
        'structure': 'geographic',
        'openiti_file': '0279Baladhuri.FutuhBuldan.Masaha002329Vols-ara1',
        'year_range': (11, 12),
        'notes': 'Contains dedicated Ridda chapters organized geographically'
    },
    'waqidi': {
        'name': 'al-Wāqidī',
        'name_ar': 'الواقدي',
        'full_title': 'Kitāb al-Ridda',
        'full_title_ar': 'كتاب الردة',
        'death_year': 207,
        'structure': 'narrative',
        'openiti_file': 'waqidi_ridda_section.txt',
        'year_range': (11, 12),
        'notes': 'Dedicated Ridda monograph, earliest source (d. 207). Ibn al-Aʿtham redaction.'
    }
}

# =============================================================================
# RIDDA WARS LEADERS AND TRIBES
# =============================================================================

# False prophets and rebel leaders (these ARE the targets in Ridda Wars)
RIDDA_LEADERS = {
    'مسيلمة': {
        'name': 'Musaylima',
        'name_ar': 'مسيلمة الكذاب',
        'tribe': 'Banū Ḥanīfa',
        'region': 'al-Yamāma',
        'epithet': 'al-Kadhdhāb',
        'notes': 'False prophet, primary target of Yamāma campaign'
    },
    'الأسود العنسي': {
        'name': 'al-Aswad al-ʿAnsī',
        'name_ar': 'الأسود العنسي',
        'tribe': 'ʿAns',
        'region': 'Yemen',
        'epithet': 'Dhū al-Khimār',
        'notes': 'False prophet in Yemen, killed before Abū Bakr\'s campaigns'
    },
    'طليحة': {
        'name': 'Ṭulayḥa ibn Khuwaylid',
        'name_ar': 'طليحة بن خويلد',
        'tribe': 'Banū Asad',
        'region': 'Najd',
        'epithet': 'al-Asadī',
        'notes': 'False prophet, later converted to Islam'
    },
    'سجاح': {
        'name': 'Sajāḥ',
        'name_ar': 'سجاح بنت الحارث',
        'tribe': 'Banū Tamīm',
        'region': 'al-Yamāma',
        'epithet': 'bint al-Ḥārith',
        'notes': 'Female prophetess, married Musaylima'
    },
    'لقيط بن مالك': {
        'name': 'Laqīṭ ibn Mālik',
        'name_ar': 'لقيط بن مالك الأزدي',
        'tribe': 'Azd',
        'region': 'Oman',
        'epithet': 'Dhū al-Tāj',
        'notes': 'Led apostasy in Oman'
    }
}

# Muslim commanders in Ridda Wars
RIDDA_COMMANDERS = {
    'خالد بن الوليد': {
        'name': 'Khālid ibn al-Walīd',
        'name_ar': 'خالد بن الوليد',
        'campaigns': ['Ṭulayḥa', 'Mālik ibn Nuwayra', 'Yamāma'],
        'epithet': 'Sayf Allāh'
    },
    'عكرمة بن أبي جهل': {
        'name': 'ʿIkrima ibn Abī Jahl',
        'name_ar': 'عكرمة بن أبي جهل',
        'campaigns': ['Oman', 'Mahra', 'Yemen'],
        'epithet': ''
    },
    'شرحبيل بن حسنة': {
        'name': 'Shurahbīl ibn Ḥasana',
        'name_ar': 'شرحبيل بن حسنة',
        'campaigns': ['Yamāma'],
        'epithet': ''
    },
    'المهاجر بن أبي أمية': {
        'name': 'al-Muhājir ibn Abī Umayya',
        'name_ar': 'المهاجر بن أبي أمية',
        'campaigns': ['Yemen'],
        'epithet': ''
    },
    'حذيفة بن محصن': {
        'name': 'Ḥudhayfa ibn Miḥṣan',
        'name_ar': 'حذيفة بن محصن',
        'campaigns': ['Oman'],
        'epithet': 'al-Ghālifānī'
    },
    'العلاء بن الحضرمي': {
        'name': 'al-ʿAlāʾ ibn al-Ḥaḍramī',
        'name_ar': 'العلاء بن الحضرمي',
        'campaigns': ['al-Baḥrayn'],
        'epithet': ''
    },
    'سويد بن مقرن': {
        'name': 'Suwayd ibn Muqarrin',
        'name_ar': 'سويد بن مقرن',
        'campaigns': ['Tihāma'],
        'epithet': ''
    },
    'عرفجة بن هرثمة': {
        'name': 'ʿArfaja ibn Harthama',
        'name_ar': 'عرفجة بن هرثمة',
        'campaigns': ['Mahra'],
        'epithet': ''
    }
}

# Major tribes involved in Ridda Wars
RIDDA_TRIBES = {
    'بنو حنيفة': {
        'name': 'Banū Ḥanīfa',
        'region': 'al-Yamāma',
        'leader': 'Musaylima',
        'outcome': 'SUBJUGATION',
        'battle': 'ʿAqrabāʾ'
    },
    'بنو أسد': {
        'name': 'Banū Asad',
        'region': 'Najd',
        'leader': 'Ṭulayḥa',
        'outcome': 'SUBJUGATION',
        'battle': 'Buzākha'
    },
    'بنو تميم': {
        'name': 'Banū Tamīm',
        'region': 'Najd',
        'leader': 'Mālik ibn Nuwayra (disputed)',
        'outcome': 'MIXED',
        'notes': 'Controversial - Mālik\'s status disputed'
    },
    'بنو غطفان': {
        'name': 'Banū Ghaṭafān',
        'region': 'Najd',
        'leader': 'ʿUyayna ibn Ḥiṣn',
        'outcome': 'SUBJUGATION',
        'battle': 'Buzākha'
    },
    'بنو سليم': {
        'name': 'Banū Sulaym',
        'region': 'Najd',
        'leader': None,
        'outcome': 'SUBMISSION',
        'notes': 'Returned quickly'
    },
    'بنو عامر': {
        'name': 'Banū ʿĀmir',
        'region': 'Najd',
        'leader': None,
        'outcome': 'MIXED',
    },
    'عنس': {
        'name': 'ʿAns',
        'region': 'Yemen',
        'leader': 'al-Aswad al-ʿAnsī',
        'outcome': 'SUBJUGATION',
    },
    'كندة': {
        'name': 'Kinda',
        'region': 'Ḥaḍramawt',
        'leader': 'al-Ashʿath ibn Qays',
        'outcome': 'SUBJUGATION',
    },
    'الأزد': {
        'name': 'Azd',
        'region': 'Oman',
        'leader': 'Laqīṭ ibn Mālik',
        'outcome': 'SUBJUGATION',
    },
    'عبد القيس': {
        'name': 'ʿAbd al-Qays',
        'region': 'al-Baḥrayn',
        'leader': 'al-Jārūd',
        'outcome': 'SUBMISSION',
        'notes': 'Remained loyal under al-Jārūd'
    },
    'بكر بن وائل': {
        'name': 'Bakr ibn Wāʾil',
        'region': 'al-Baḥrayn',
        'leader': 'al-Ḥuṭam',
        'outcome': 'SUBJUGATION',
    }
}

# =============================================================================
# ARABIAN PENINSULA GEOGRAPHY (Ridda Wars)
# =============================================================================

RIDDA_REGIONS = {
    'najd': {
        'name': 'Najd',
        'name_ar': 'نجد',
        'center_lat': 24.5,
        'center_lon': 45.5,
        'tribes': ['Banū Asad', 'Banū Tamīm', 'Banū Ghaṭafān', 'Banū Sulaym'],
        'key_sites': ['Buzākha', 'al-Buṭāḥ']
    },
    'yamama': {
        'name': 'al-Yamāma',
        'name_ar': 'اليمامة',
        'center_lat': 24.0,
        'center_lon': 46.7,
        'tribes': ['Banū Ḥanīfa'],
        'key_sites': ['ʿAqrabāʾ', 'al-Yamāma']
    },
    'yemen': {
        'name': 'Yemen',
        'name_ar': 'اليمن',
        'center_lat': 15.5,
        'center_lon': 44.2,
        'tribes': ['ʿAns', 'Madhḥij'],
        'key_sites': ['Ṣanʿāʾ', 'Najrān']
    },
    'hadramawt': {
        'name': 'Ḥaḍramawt',
        'name_ar': 'حضرموت',
        'center_lat': 15.9,
        'center_lon': 49.0,
        'tribes': ['Kinda'],
        'key_sites': ['Ḥaḍramawt']
    },
    'oman': {
        'name': 'Oman',
        'name_ar': 'عمان',
        'center_lat': 23.0,
        'center_lon': 57.0,
        'tribes': ['Azd'],
        'key_sites': ['Dabā', 'Ṣuḥār']
    },
    'bahrayn': {
        'name': 'al-Baḥrayn',
        'name_ar': 'البحرين',
        'center_lat': 26.0,
        'center_lon': 50.5,
        'tribes': ['ʿAbd al-Qays', 'Bakr ibn Wāʾil'],
        'key_sites': ['Hajar', 'al-Qaṭīf']
    },
    'mahra': {
        'name': 'Mahra',
        'name_ar': 'مهرة',
        'center_lat': 16.5,
        'center_lon': 52.0,
        'tribes': ['Mahra'],
        'key_sites': ['Mahra']
    },
    'tihama': {
        'name': 'Tihāma',
        'name_ar': 'تهامة',
        'center_lat': 20.0,
        'center_lon': 41.0,
        'tribes': ['Various'],
        'key_sites': ['Tihāma']
    }
}

# Key battle sites with al-Turayyā coordinates
RIDDA_BATTLE_SITES = {
    'عقرباء': {
        'name': 'ʿAqrabāʾ',
        'name_ar': 'عقرباء',
        'region': 'al-Yamāma',
        'lat': 24.0,
        'lon': 46.7,
        'battle': 'Battle of Yamāma',
        'year_ah': 12,
        'commander': 'Khālid ibn al-Walīd',
        'opponent': 'Musaylima'
    },
    'بزاخة': {
        'name': 'Buzākha',
        'name_ar': 'بزاخة',
        'region': 'Najd',
        'lat': 27.5,
        'lon': 41.5,
        'battle': 'Battle of Buzākha',
        'year_ah': 11,
        'commander': 'Khālid ibn al-Walīd',
        'opponent': 'Ṭulayḥa'
    },
    'البطاح': {
        'name': 'al-Buṭāḥ',
        'name_ar': 'البطاح',
        'region': 'Najd',
        'lat': 26.5,
        'lon': 44.0,
        'battle': 'Execution of Mālik ibn Nuwayra',
        'year_ah': 11,
        'commander': 'Khālid ibn al-Walīd',
        'opponent': 'Mālik ibn Nuwayra'
    },
    'دبا': {
        'name': 'Dabā',
        'name_ar': 'دبا',
        'region': 'Oman',
        'lat': 25.6,
        'lon': 56.3,
        'battle': 'Battle of Dabā',
        'year_ah': 11,
        'commander': 'Ḥudhayfa ibn Miḥṣan / ʿIkrima',
        'opponent': 'Laqīṭ ibn Mālik'
    },
    'صنعاء': {
        'name': 'Ṣanʿāʾ',
        'name_ar': 'صنعاء',
        'region': 'Yemen',
        'lat': 15.35,
        'lon': 44.21,
        'notes': 'Capital held by al-Aswad al-ʿAnsī'
    }
}

# =============================================================================
# ARABIC TEXT PROCESSING
# =============================================================================

def normalize_arabic(text: str) -> str:
    """Basic Arabic text normalization."""
    if not text:
        return ''
    text = re.sub(r'[\u064B-\u065F\u0670]', '', text)
    text = re.sub(r'[إأآٱ]', 'ا', text)
    text = re.sub(r'[ىئ]', 'ي', text)
    text = re.sub(r'ة', 'ه', text)
    return text


# =============================================================================
# INCORPORATION MODE DETECTION
# =============================================================================

# Arabic terms for SUBJUGATION (qitāl) - Military action
QITAL_TERMS = [
    'قتال', 'قاتل', 'قاتلهم', 'قاتلوا', 'قتالا شديدا',
    'حرب', 'حارب', 'حاربهم',
    'غزو', 'غزا', 'غزوة',
    'سيف', 'بالسيف', 'وضع السيف',
    'قتل', 'قتلوا', 'قتل منهم', 'قتل خلقا',
    'هزم', 'هزمهم', 'هزيمة', 'انهزموا',
    'ظفر', 'ظفر بهم', 'نصر عليهم',
    'أوقع بهم', 'وقعة',
    'غنم', 'غنيمة', 'سبي', 'سبايا',
    'فتح', 'افتتح', 'افتتحها',
    'حصار', 'حاصر', 'حاصرهم',
    'اقتحم', 'اقتحموا'
]

# Arabic terms for SUBMISSION (ṭāʿa) - Voluntary return
TAA_TERMS = [
    'طاعة', 'أطاع', 'أطاعوا', 'دخلوا في الطاعة',
    'رجع', 'رجعوا', 'رجع إلى الإسلام',
    'تاب', 'تابوا', 'توبة',
    'أسلم', 'أسلموا', 'عاد إلى الإسلام',
    'بايع', 'بايعوا', 'بيعة',
    'صالح', 'صلح', 'صالحهم',
    'أمان', 'أمن', 'استأمن',
    'سلم', 'سلموا', 'استسلم',
    'أذعن', 'أذعنوا', 'خضع',
    'دخل في الإسلام',
    'أدى الزكاة', 'الزكاة'
]

# Ridda-specific terms
RIDDA_TERMS = [
    'ردة', 'ارتد', 'ارتدوا', 'مرتد', 'مرتدون',
    'كفر', 'كفروا', 'كافر',
    'منع الزكاة', 'منعوا الزكاة',
    'تنبأ', 'متنبئ', 'ادعى النبوة',
    'الكذاب', 'كذاب'
]


def detect_incorporation_terms(text: str) -> Dict[str, List[str]]:
    """Detect incorporation mode terms in Arabic text."""
    text_norm = normalize_arabic(text)
    
    found_qital = [t for t in QITAL_TERMS if t in text or normalize_arabic(t) in text_norm]
    found_taa = [t for t in TAA_TERMS if t in text or normalize_arabic(t) in text_norm]
    found_ridda = [t for t in RIDDA_TERMS if t in text or normalize_arabic(t) in text_norm]
    
    return {
        'qital_terms': found_qital,
        'taa_terms': found_taa,
        'ridda_terms': found_ridda
    }


def classify_incorporation_mode(terms: Dict[str, List[str]]) -> str:
    """Classify incorporation mode based on detected Arabic terms."""
    has_qital = len(terms.get('qital_terms', [])) > 0
    has_taa = len(terms.get('taa_terms', [])) > 0
    
    if has_qital and has_taa:
        return 'MIXED'
    elif has_qital:
        return 'SUBJUGATION'
    elif has_taa:
        return 'SUBMISSION'
    else:
        return 'UNKNOWN'


def normalize_incorporation_mode(raw_mode: str) -> str:
    """Normalize incorporation mode to standard terminology."""
    if not raw_mode:
        return 'UNKNOWN'
    
    raw_upper = raw_mode.upper().strip()
    
    if raw_upper in ['SUBJUGATION', 'QITAL', 'QITĀL', 'قتال', 'MILITARY']:
        return 'SUBJUGATION'
    if raw_upper in ['SUBMISSION', 'TAA', 'ṬĀʿA', 'TAAA', 'طاعة', 'VOLUNTARY']:
        return 'SUBMISSION'
    if raw_upper in ['MIXED', 'BOTH', 'COMBINED']:
        return 'MIXED'
    
    return 'UNKNOWN'


# =============================================================================
# BRILL EI3 TRANSLITERATION
# =============================================================================

BRILL_TRANSLITERATIONS = {
    # Regions
    'اليمامة': 'al-Yamāma',
    'نجد': 'Najd',
    'اليمن': 'al-Yaman',
    'عمان': 'ʿUmān',
    'البحرين': 'al-Baḥrayn',
    'حضرموت': 'Ḥaḍramawt',
    'مهرة': 'Mahra',
    'تهامة': 'Tihāma',
    
    # Battle sites
    'عقرباء': 'ʿAqrabāʾ',
    'بزاخة': 'Buzākha',
    'البطاح': 'al-Buṭāḥ',
    'دبا': 'Dabā',
    'صنعاء': 'Ṣanʿāʾ',
    
    # Leaders
    'مسيلمة': 'Musaylima',
    'طليحة': 'Ṭulayḥa',
    'سجاح': 'Sajāḥ',
    'الأسود العنسي': 'al-Aswad al-ʿAnsī',
    'لقيط بن مالك': 'Laqīṭ ibn Mālik',
    'مالك بن نويرة': 'Mālik ibn Nuwayra',
    
    # Commanders
    'خالد بن الوليد': 'Khālid ibn al-Walīd',
    'عكرمة بن أبي جهل': 'ʿIkrima ibn Abī Jahl',
    'شرحبيل بن حسنة': 'Shurahbīl ibn Ḥasana',
    'المهاجر بن أبي أمية': 'al-Muhājir ibn Abī Umayya',
    'حذيفة بن محصن': 'Ḥudhayfa ibn Miḥṣan',
    'العلاء بن الحضرمي': 'al-ʿAlāʾ ibn al-Ḥaḍramī',
    
    # Tribes
    'بنو حنيفة': 'Banū Ḥanīfa',
    'بنو أسد': 'Banū Asad',
    'بنو تميم': 'Banū Tamīm',
    'بنو غطفان': 'Banū Ghaṭafān',
    'بنو سليم': 'Banū Sulaym',
    'كندة': 'Kinda',
    'الأزد': 'Azd',
    'عبد القيس': 'ʿAbd al-Qays',
    'بكر بن وائل': 'Bakr ibn Wāʾil',
    'عنس': 'ʿAns',
    
    # Caliphs
    'أبو بكر': 'Abū Bakr',
    'أبو بكر الصديق': 'Abū Bakr al-Ṣiddīq',
}


def transliterate_brill(text: str) -> str:
    """Apply Brill EI3 transliteration to Arabic text."""
    if not text:
        return text
    
    if text in BRILL_TRANSLITERATIONS:
        return BRILL_TRANSLITERATIONS[text]
    
    text_norm = normalize_arabic(text)
    for ar, en in BRILL_TRANSLITERATIONS.items():
        if normalize_arabic(ar) == text_norm:
            return en
    
    return text


# =============================================================================
# YEAR DETECTION (Ridda Wars specific: 11-12 AH)
# =============================================================================

YEAR_PATTERNS = [
    (r'سنة\s*(\d+)', 1),
    (r'في\s*سنة\s*(\d+)', 1),
    (r'ثم\s*دخلت\s*سنة\s*(\d+)', 1),
    (r'(\d+)\s*(?:من\s*)?(?:ال)?هجر[ةه]', 1),
]

# Arabic written numbers for Ridda Wars period
ARABIC_WRITTEN_YEARS = {
    # Year 11 - various spellings
    'إحدى عشرة': 11,
    'احدى عشرة': 11,
    'إحدى عشر': 11,
    'احدى عشر': 11,
    'احدي عشرة': 11,
    'إحدي عشرة': 11,
    # Year 12 - various spellings
    'اثنتي عشرة': 12,
    'اثنتى عشرة': 12,
    'اثنتي عشر': 12,
    'اثنى عشرة': 12,
    'اثني عشرة': 12,
    'ثنتي عشرة': 12,
    'اثنتا عشرة': 12,
    'اثنتا عشر': 12,
}


def detect_year_in_text(text: str) -> Optional[int]:
    """Detect Hijri year in text (11-12 AH for Ridda Wars)."""
    import unicodedata
    
    # Normalize text for comparison
    normalized_text = unicodedata.normalize('NFC', text)
    
    # First check for Arabic written numbers
    for written, year in ARABIC_WRITTEN_YEARS.items():
        normalized_written = unicodedata.normalize('NFC', written)
        if normalized_written in normalized_text:
            return year
    
    # Also try with diacritics removed
    text_no_diacritics = normalize_arabic(text)
    for written, year in ARABIC_WRITTEN_YEARS.items():
        written_no_diacritics = normalize_arabic(written)
        if written_no_diacritics in text_no_diacritics:
            return year
    
    # Then check numeric patterns
    for pattern, group in YEAR_PATTERNS:
        match = re.search(pattern, text)
        if match:
            try:
                year = int(match.group(group))
                # Ridda Wars: only 11-12 AH
                if 11 <= year <= 12:
                    return year
            except (ValueError, IndexError):
                pass
    return None


# =============================================================================
# CONFIGURATION LOADER
# =============================================================================

class ConfigLoader:
    """Load and manage configuration from YAML files."""
    
    def __init__(self, config_dir: Path = None):
        self.config_dir = config_dir or Path('.')
        self.config = {}
        self.tribes = {}
        self.prompts = {}
        self._load_configs()
    
    def _load_configs(self):
        """Load all configuration files."""
        # Load main config
        config_path = self.config_dir / 'ridda_config.yaml'
        if config_path.exists() and YAML_AVAILABLE:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f) or {}
            logger.info(f"Loaded config from {config_path}")
        
        # Load tribes gazetteer
        tribes_path = self.config_dir / 'ridda_tribes.yaml'
        if tribes_path.exists() and YAML_AVAILABLE:
            with open(tribes_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}
                self.tribes = data.get('tribes', data)
            logger.info(f"Loaded {len(self.tribes)} tribes from gazetteer")
        
        # Load prompts
        prompts_path = self.config_dir / 'ridda_prompts.yaml'
        if prompts_path.exists() and YAML_AVAILABLE:
            with open(prompts_path, 'r', encoding='utf-8') as f:
                self.prompts = yaml.safe_load(f) or {}
            logger.info(f"Loaded prompts from {prompts_path}")
    
    def get_api_config(self) -> Dict:
        """Get API configuration."""
        return self.config.get('api', {}).get('anthropic', {})
    
    def get_source_config(self, source_name: str) -> Dict:
        """Get configuration for a specific source."""
        sources = self.config.get('sources', {})
        if source_name in sources:
            return sources[source_name]
        return RIDDA_SOURCES.get(source_name, {})
    
    def get_extraction_config(self) -> Dict:
        """Get extraction settings."""
        defaults = {
            'min_year': 11,
            'max_year': 12,  # Ridda Wars period
            'chunk_size': 20000,
            'overlap': 2000,
            'delay': 1.5,
            'max_retries': 3
        }
        return {**defaults, **self.config.get('extraction', {})}
    
    def get_tribe_info(self, tribe_arabic: str) -> Optional[Dict]:
        """Look up tribe in gazetteer."""
        if not tribe_arabic:
            return None
        
        tribe_norm = normalize_arabic(tribe_arabic)
        
        for name, info in RIDDA_TRIBES.items():
            if tribe_arabic == name or tribe_norm == normalize_arabic(name):
                return info
        
        return None


# =============================================================================
# SOURCE FILE LOADING
# =============================================================================

def load_source_text(source_name: str, config_loader: ConfigLoader, 
                     data_dir: Path = None) -> Tuple[Optional[str], str]:
    """Load source text from OpenITI corpus."""
    data_dir = data_dir or Path('data')
    
    # First try to get from config file (preferred), then fall back to hardcoded
    source_info = {}
    if config_loader and config_loader.config:
        sources_config = config_loader.config.get('sources', {})
        source_info = sources_config.get(source_name, {})
    
    # Fall back to hardcoded RIDDA_SOURCES if not in config
    if not source_info:
        source_info = RIDDA_SOURCES.get(source_name, {})
    
    openiti_file = source_info.get('openiti_file')
    
    if not openiti_file:
        logger.error(f"No file configured for source: {source_name}")
        return None, ""
    
    print(f"  📄 Config file: {openiti_file}")
    
    # Try various locations
    for possible_path in [
        data_dir / openiti_file,
        Path(openiti_file),
        data_dir.parent / openiti_file,
    ]:
        if possible_path.exists():
            filepath = possible_path
            break
    else:
        logger.error(f"File not found: {openiti_file}")
        logger.error(f"  Searched in: {data_dir}")
        return None, str(openiti_file)
    
    print(f"  📖 Loading: {filepath}")
    with open(filepath, 'r', encoding='utf-8') as f:
        text = f.read()
    
    original_len = len(text)
    
    # Clean OpenITI markup
    text = re.sub(r'PageV\d+P\d+', '', text)
    text = re.sub(r'ms\d+', '', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    # Remove header section
    header_end = text.find('#META#Header#End#')
    if header_end != -1:
        text = text[header_end + len('#META#Header#End#'):]
    
    print(f"  📊 Size: {original_len:,} → {len(text):,} chars")
    return text, filepath.name


def filter_ridda_sections(text: str) -> str:
    """Filter text to keep only Ridda Wars relevant sections (years 11-12 AH)."""
    # Look for Ridda-specific markers
    ridda_markers = [
        'ردة', 'الردة', 'حروب الردة', 'أهل الردة',
        'مسيلمة', 'طليحة', 'سجاح', 'الأسود العنسي',
        'سنة إحدى عشرة', 'سنة اثنتي عشرة',
        'سنة 11', 'سنة 12',
        'خلافة أبي بكر', 'أبو بكر الصديق'
    ]
    
    # For now, return all text - filtering can be done during extraction
    # A more sophisticated approach would segment by year
    return text


# =============================================================================
# LLM EXTRACTION
# =============================================================================

def get_extraction_prompt(config_loader: ConfigLoader, source_name: str,
                          text: str, detected_year: Optional[int]) -> str:
    """Generate extraction prompt for Ridda Wars events."""
    source_config = config_loader.get_source_config(source_name)
    structure = source_config.get('structure', 'annalistic')
    
    year_hint = f"Current section: ~{detected_year} AH." if detected_year else "Year: 11-12 AH (Ridda Wars period)"
    
    # Source-specific context
    source_context = ""
    if source_name == 'waqidi':
        source_context = """
SOURCE: al-Wāqidī's Kitāb al-Ridda (d. 207/823) — the earliest dedicated Ridda monograph.
This text has a narrative structure with extended battle accounts, poetry (ignore poems),
and detailed isnād chains (skip transmission chains, extract only historical events).
The text is transmitted via Ibn al-Aʿtham al-Kūfī. It contains rich detail on Najd,
Yamāma (Musaylima), Bahrain, and Ḥaḍramawt campaigns but does NOT cover al-Aswad al-ʿAnsī
in Yemen. Pay attention to the Saqīfa section — it provides context but is NOT an
incorporation event itself.
"""
    elif source_name == 'tabari':
        source_context = """
SOURCE: al-Ṭabarī's Tārīkh (d. 310/923) — annalistic structure organized by year.
"""
    elif source_name == 'baladhuri':
        source_context = """
SOURCE: al-Balādhurī's Futūḥ al-Buldān (d. 279/892) — geographic structure organized by region.
"""
    
    return f"""You are an expert in early Islamic history, specializing in the Ridda Wars (حروب الردة) 
of 11-12 AH / 632-633 CE - the "Wars of Apostasy" following the death of Prophet Muhammad.

Extract TRIBAL INTERACTION EVENTS from this Arabic text about the Ridda Wars.

{year_hint}
{source_context}
CONTEXT: After the Prophet's death, many Arabian tribes withdrew from Islam (ردة) or refused 
to pay zakāt. Caliph Abū Bakr sent military expeditions to restore authority over the Peninsula.

EXTRACT: Events describing:
1. Military campaigns (qitāl) against rebellious tribes
2. Voluntary submissions (ṭāʿa) of tribes returning to Islam
3. Battles, sieges, negotiations with apostate tribes
4. Actions of false prophets: Musaylima, Ṭulayḥa, Sajāḥ, al-Aswad al-ʿAnsī

REQUIRED FIELDS:
- tribe_arabic: Tribe name in Arabic (e.g., بنو حنيفة)
- tribe_english: Brill EI3 transliteration (e.g., Banū Ḥanīfa)
- region_arabic: Region in Arabic (e.g., اليمامة)
- region_english: Region transliterated (e.g., al-Yamāma)
- year_ah: Hijri year (11 or 12)
- incorporation_mode: One of:
  * "SUBJUGATION" = qitāl - military campaign, battle, armed conflict
  * "SUBMISSION" = ṭāʿa - voluntary return to Islam, negotiated peace
  * "MIXED" = both elements present
- commander_arabic: Muslim commander name in Arabic
- commander_english: Brill EI3 transliteration
- rebel_leader_arabic: Rebel/false prophet name (if applicable)
- rebel_leader_english: Transliterated
- battle_site_arabic: Battle location if mentioned
- battle_site_english: Transliterated
- evidence: Array of Arabic quotes supporting classification
- confidence: 0.0 to 1.0
- notes: Any relevant details

INCORPORATION MODE INDICATORS:
SUBJUGATION (qitāl): قتال، قاتل، حرب، غزا، سيف، قتل، هزم، ظفر، غنم، سبي، فتح، حصار
SUBMISSION (ṭāʿa): طاعة، أطاع، رجع، تاب، أسلم، بايع، صالح، أمان، سلم، أدى الزكاة

RIDDA-SPECIFIC TERMS:
ردة، ارتد، مرتد، منع الزكاة، تنبأ، متنبئ، الكذاب

KEY FIGURES TO WATCH FOR:
- False prophets: مسيلمة (Musaylima), طليحة (Ṭulayḥa), سجاح (Sajāḥ), الأسود العنسي (al-Aswad)
- Commanders: خالد بن الوليد (Khālid), عكرمة (ʿIkrima), المهاجر (al-Muhājir), العلاء (al-ʿAlāʾ)
- Tribes: بنو حنيفة, بنو أسد, بنو تميم, كندة, الأزد

BRILL EI3 TRANSLITERATION:
- اليمامة → al-Yamāma
- بنو حنيفة → Banū Ḥanīfa
- خالد بن الوليد → Khālid ibn al-Walīd
- مسيلمة → Musaylima

If NO Ridda Wars event found: {{"events": [], "skipped": ["reason"]}}

TEXT:
---
{text[:18000]}
---

JSON only: {{"events": [...], "skipped": [...]}}"""


def extract_with_claude(client, config_loader: ConfigLoader, text: str,
                        source: str, detected_year: Optional[int],
                        extraction_config: Dict) -> Tuple[List[dict], List[str]]:
    """Extract Ridda Wars events using Claude API."""
    
    source_config = config_loader.get_source_config(source) or {}
    prompt = get_extraction_prompt(config_loader, source, text, detected_year)
    
    for attempt in range(extraction_config.get('max_retries', 3)):
        try:
            model = config_loader.get_api_config().get('model', 'claude-sonnet-4-20250514')
            response = client.messages.create(
                model=model, max_tokens=4000,
                messages=[{"role": "user", "content": prompt}]
            )
            content = response.content[0].text.strip()
            
            if content.startswith('```'):
                content = re.sub(r'^```json?\s*', '', content)
                content = re.sub(r'\s*```$', '', content)
            
            data = json.loads(content)
            
            # Handle various response formats
            if 'events' in data:
                events = data.get('events', [])
                skipped = data.get('skipped', [])
            elif 'tribe' in data or 'tribe_arabic' in data:
                events = [data]
                skipped = []
            elif 'error' in data:
                return [], [data.get('error')]
            else:
                events = []
                skipped = []
            
            processed = []
            min_year, max_year = 11, 12  # Ridda Wars period
            
            for evt in events:
                evt['_source'] = source
                
                # Normalize field names
                if 'tribe' in evt and 'tribe_english' not in evt:
                    evt['tribe_english'] = evt.pop('tribe')
                
                # Normalize incorporation mode
                evt['incorporation_mode'] = normalize_incorporation_mode(
                    evt.get('incorporation_mode', evt.get('mode', ''))
                )
                
                # Apply Brill transliterations
                if evt.get('tribe_english'):
                    evt['tribe_english'] = transliterate_brill(evt['tribe_english'])
                if evt.get('commander_english'):
                    evt['commander_english'] = transliterate_brill(evt['commander_english'])
                if evt.get('region_english'):
                    evt['region_english'] = transliterate_brill(evt['region_english'])
                
                # Get tribe info from gazetteer
                tribe_info = config_loader.get_tribe_info(evt.get('tribe_arabic', ''))
                if tribe_info:
                    evt.update({
                        '_tribe_region': tribe_info.get('region'),
                        '_tribe_leader': tribe_info.get('leader'),
                    })
                
                # Validate year
                year = evt.get('year_ah') or detected_year
                if year:
                    evt['year_ah'] = year
                    if not (min_year <= year <= max_year):
                        # Outside Ridda Wars period
                        continue
                
                # Add geocoding from battle sites
                site = evt.get('battle_site_arabic', '')
                if site in RIDDA_BATTLE_SITES:
                    site_info = RIDDA_BATTLE_SITES[site]
                    evt['_lat'] = site_info.get('lat')
                    evt['_lon'] = site_info.get('lon')
                elif evt.get('region_arabic') in RIDDA_REGIONS:
                    region_info = RIDDA_REGIONS[evt['region_arabic']]
                    evt['_lat'] = region_info.get('center_lat')
                    evt['_lon'] = region_info.get('center_lon')
                
                # Detect terms for validation
                evidence_text = ' '.join(evt.get('evidence', [])) if isinstance(evt.get('evidence'), list) else str(evt.get('evidence', ''))
                evt['_detected_terms'] = detect_incorporation_terms(evidence_text)
                evt['_term_mode'] = classify_incorporation_mode(evt['_detected_terms'])
                
                processed.append(evt)
            
            return processed, skipped
            
        except json.JSONDecodeError:
            if attempt < 2:
                time.sleep(2)
                continue
            return [], ["JSON error"]
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
                continue
            return [], [f"API error: {str(e)[:100]}"]
    
    return [], ["Max retries"]


# =============================================================================
# MAIN EXTRACTION FUNCTION
# =============================================================================

def extract_source(source_name: str, config_loader: ConfigLoader, client,
                   output_dir: Path, data_dir: Path,
                   start_chunk: int = 0, max_chunks: Optional[int] = None,
                   resume: bool = True) -> Dict:
    """Extract Ridda Wars events from a source."""
    
    print(f"\n{'='*70}")
    print(f"Processing: {source_name}")
    print(f"{'='*70}")
    
    source_config = config_loader.get_source_config(source_name)
    extraction_config = config_loader.get_extraction_config()
    
    # Load source text
    text, filename = load_source_text(source_name, config_loader, data_dir)
    if not text:
        return {'error': f'Could not load source file for {source_name}'}
    
    # Filter for Ridda Wars sections
    text = filter_ridda_sections(text)
    
    # Chunk the text
    chunk_size = extraction_config.get('chunk_size', 20000)
    overlap = extraction_config.get('overlap', 2000)
    delay = extraction_config.get('delay', 1.5)
    
    chunks = []
    pos = 0
    while pos < len(text):
        end = min(pos + chunk_size, len(text))
        chunks.append(text[pos:end])
        pos = end - overlap if end < len(text) else end
    
    print(f"  📊 Created {len(chunks)} chunks")
    
    # Check for checkpoint
    checkpoint_path = output_dir / f'.checkpoint_ridda_{source_name}.json'
    all_events = []
    processed_chunks = 0
    skipped_chunks = []
    
    if resume and checkpoint_path.exists():
        try:
            with open(checkpoint_path, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
                all_events = checkpoint.get('events', [])
                processed_chunks = checkpoint.get('processed_chunks', 0)
                skipped_chunks = checkpoint.get('skipped_chunks', [])
            print(f"  📍 Resuming from checkpoint:")
            print(f"      - Processed chunks: {processed_chunks}")
            print(f"      - Events found: {len(all_events)}")
            print(f"      - Skipped chunks: {len(skipped_chunks)}")
        except Exception as e:
            print(f"  ⚠️ Could not load checkpoint: {e}")
            print(f"      Starting fresh...")
    
    # Process chunks
    if max_chunks:
        end_chunk = min(start_chunk + max_chunks, len(chunks))
    else:
        end_chunk = len(chunks)
    
    start_idx = max(processed_chunks, start_chunk)
    
    # Count Ridda-relevant chunks first
    ridda_chunks = []
    for i, chunk in enumerate(chunks):
        has_ridda = any(term in chunk for term in RIDDA_TERMS)
        if has_ridda:
            ridda_chunks.append(i)
    
    print(f"  📋 Chunks with Ridda content: {len(ridda_chunks)}/{len(chunks)}")
    
    try:
        for i in range(start_idx, end_chunk):
            chunk = chunks[i]
            detected_year = detect_year_in_text(chunk)
            
            # Check for Ridda content
            has_ridda = any(term in chunk for term in RIDDA_TERMS)
            if not has_ridda:
                # Skip chunks without Ridda content
                if i not in skipped_chunks:
                    skipped_chunks.append(i)
                continue
            
            # Calculate progress
            remaining = end_chunk - i
            progress_pct = ((i - start_idx + 1) / (end_chunk - start_idx)) * 100
            
            print(f"  🔄 Chunk {i+1}/{len(chunks)} (year: {detected_year or '?'}) [{progress_pct:.1f}%]")
            
            try:
                events, skipped = extract_with_claude(
                    client, config_loader, chunk, source_name, detected_year, extraction_config
                )
                
                if events:
                    all_events.extend(events)
                    print(f"      ✅ Extracted {len(events)} events (total: {len(all_events)})")
                
                if skipped:
                    print(f"      ⏭️  Skipped: {skipped[0][:50]}...")
                
            except Exception as e:
                print(f"      ❌ Error: {e}")
                print(f"      Saving checkpoint and continuing...")
            
            # Save checkpoint after each chunk
            checkpoint_data = {
                'events': all_events,
                'processed_chunks': i + 1,
                'skipped_chunks': skipped_chunks,
                'total_chunks': len(chunks),
                'last_updated': datetime.now().isoformat(),
                'source': source_name
            }
            with open(checkpoint_path, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
            
            time.sleep(delay)
    
    except KeyboardInterrupt:
        print(f"\n  ⚠️ Interrupted! Progress saved to checkpoint.")
        print(f"      Run again with same command to resume.")
        return None
    
    # Prepare output
    output = {
        'source': source_name,
        'source_info': source_config,
        'count': len(all_events),
        'version': '1.0',
        'extracted_at': datetime.now().isoformat(),
        'terminology': {
            'SUBJUGATION': 'qitāl - Military subjugation',
            'SUBMISSION': 'ṭāʿa - Voluntary submission',
            'MIXED': 'Combined elements'
        },
        'events': all_events
    }
    
    # Save output JSON
    output_path = output_dir / f'ridda_{source_name}.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    # Save CSV with ALL bilingual fields
    csv_path = output_dir / f'ridda_{source_name}.csv'
    csv_fields = [
        # Tribe (bilingual)
        'tribe_arabic', 'tribe_english',
        # Region (bilingual)
        'region_arabic', 'region_english',
        # Year (both calendars)
        'year_ah', 'year_ce',
        # Incorporation mode (bilingual)
        'incorporation_mode_arabic', 'incorporation_mode_english',
        # Commander (bilingual)
        'commander_arabic', 'commander_english',
        # Rebel leader (bilingual)
        'rebel_leader_arabic', 'rebel_leader_english',
        # Battle site (bilingual)
        'battle_site_arabic', 'battle_site_english',
        # Outcome (bilingual)
        'outcome_arabic', 'outcome_english',
        # Evidence (bilingual)
        'evidence_arabic', 'evidence_english',
        # Notes (bilingual)
        'notes_arabic', 'notes_english',
        # Confidence and geocoding
        'confidence', '_lat', '_lon', '_source'
    ]
    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields, extrasaction='ignore')
        writer.writeheader()
        for evt in all_events:
            # Handle legacy field names for backward compatibility
            inc_mode_ar = evt.get('incorporation_mode_arabic', '')
            inc_mode_en = evt.get('incorporation_mode_english', evt.get('incorporation_mode', ''))
            
            # Convert evidence arrays to strings
            evidence_ar = evt.get('evidence_arabic', evt.get('evidence', []))
            evidence_en = evt.get('evidence_english', [])
            if isinstance(evidence_ar, list):
                evidence_ar = ' | '.join(evidence_ar)
            if isinstance(evidence_en, list):
                evidence_en = ' | '.join(evidence_en)
            
            row = {
                'tribe_arabic': evt.get('tribe_arabic', ''),
                'tribe_english': evt.get('tribe_english', ''),
                'region_arabic': evt.get('region_arabic', ''),
                'region_english': evt.get('region_english', ''),
                'year_ah': evt.get('year_ah', ''),
                'year_ce': evt.get('year_ce', ''),
                'incorporation_mode_arabic': inc_mode_ar,
                'incorporation_mode_english': inc_mode_en,
                'commander_arabic': evt.get('commander_arabic', ''),
                'commander_english': evt.get('commander_english', ''),
                'rebel_leader_arabic': evt.get('rebel_leader_arabic', ''),
                'rebel_leader_english': evt.get('rebel_leader_english', ''),
                'battle_site_arabic': evt.get('battle_site_arabic', ''),
                'battle_site_english': evt.get('battle_site_english', ''),
                'outcome_arabic': evt.get('outcome_arabic', ''),
                'outcome_english': evt.get('outcome_english', ''),
                'evidence_arabic': evidence_ar,
                'evidence_english': evidence_en,
                'notes_arabic': evt.get('notes_arabic', evt.get('notes', '')),
                'notes_english': evt.get('notes_english', ''),
                'confidence': evt.get('confidence', ''),
                '_lat': evt.get('_lat', ''),
                '_lon': evt.get('_lon', ''),
                '_source': evt.get('_source', source_name),
            }
            writer.writerow(row)
    
    print(f"\n  ✅ Saved: {output_path}")
    print(f"  ✅ Saved: {csv_path}")
    print(f"  📊 Events: {len(all_events)}")
    
    # Clean up checkpoint
    if checkpoint_path.exists():
        checkpoint_path.unlink()
    
    return output


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Ridda Wars Database Pipeline v1.1',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ridda Wars Database - Tribal Unification Analysis (11-12 AH / 632-633 CE)

Sources:
  tabari     - al-Ṭabarī's Tārīkh al-Rusul wa-l-Mulūk (d. 310)
  baladhuri  - al-Balādhurī's Futūḥ al-Buldān (d. 279)
  waqidi     - al-Wāqidī's Kitāb al-Ridda (d. 207)

Terminology (Brill EI3):
  SUBJUGATION (qitāl) - Military campaign against rebellious tribe
  SUBMISSION (ṭāʿa)   - Voluntary return to Islamic authority

Examples:
  python ridda_pipeline.py --source tabari
  python ridda_pipeline.py --source waqidi --data-dir ./data
  python ridda_pipeline.py --source all --max-chunks 50
        """
    )
    
    parser.add_argument('--source', type=str, required=True,
                        choices=list(RIDDA_SOURCES.keys()) + ['all'],
                        help='Source to extract (or "all")')
    parser.add_argument('--data-dir', type=str, default='data',
                        help='Directory containing source files')
    parser.add_argument('--output-dir', type=str, default='./output/llm_results',
                        help='Directory for output files')
    parser.add_argument('--config-dir', type=str, default='.',
                        help='Directory containing config files')
    parser.add_argument('--start-chunk', type=int, default=0,
                        help='Start from this chunk index')
    parser.add_argument('--max-chunks', type=int, default=None,
                        help='Maximum chunks to process')
    parser.add_argument('--no-resume', action='store_true',
                        help='Do not resume from checkpoint')
    parser.add_argument('--list-sources', action='store_true',
                        help='List available sources and exit')
    
    args = parser.parse_args()
    
    if args.list_sources:
        print("\nAvailable sources for Ridda Wars extraction:")
        print("-" * 60)
        for name, info in RIDDA_SOURCES.items():
            print(f"  {name:15} - {info['name']} (d. {info['death_year']})")
            print(f"                   {info['full_title']}")
            print(f"                   OpenITI: {info['openiti_file']}")
        return
    
    # Setup
    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    config_dir = Path(args.config_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load configuration
    config_loader = ConfigLoader(config_dir)
    
    # Initialize Claude client
    if not CLAUDE_AVAILABLE:
        print("ERROR: anthropic package not installed")
        sys.exit(1)
    
    api_config = config_loader.get_api_config()
    api_key = api_config.get('api_key') or os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        print("ERROR: No API key found. Set in ridda_config.yaml or ANTHROPIC_API_KEY env var")
        sys.exit(1)
    
    client = anthropic.Anthropic(api_key=api_key)
    
    # Determine sources to process
    if args.source == 'all':
        sources = list(RIDDA_SOURCES.keys())
    else:
        sources = [args.source]
    
    print("=" * 70)
    print("RIDDA WARS DATABASE PIPELINE v1.1")
    print("Tribal Unification Analysis (11-12 AH / 632-633 CE)")
    print("Sources: al-Ṭabarī · al-Balādhurī · al-Wāqidī")
    print("=" * 70)
    print(f"Sources: {', '.join(sources)}")
    print(f"Data dir: {data_dir}")
    print(f"Output dir: {output_dir}")
    
    # Process each source
    results = {}
    for source in sources:
        try:
            result = extract_source(
                source, config_loader, client, output_dir, data_dir,
                start_chunk=args.start_chunk,
                max_chunks=args.max_chunks,
                resume=not args.no_resume
            )
            results[source] = result
        except Exception as e:
            logger.error(f"Error processing {source}: {e}")
            results[source] = {'error': str(e)}
    
    # Summary
    print("\n" + "=" * 70)
    print("EXTRACTION SUMMARY")
    print("=" * 70)
    
    total_events = 0
    for source, result in results.items():
        if 'error' in result:
            print(f"  ❌ {source}: {result['error']}")
        else:
            count = result.get('count', 0)
            print(f"  ✅ {source}: {count} tribal interaction events")
            total_events += count
    
    print(f"\n  Total: {total_events} events")
    print("\n✅ Done!")


if __name__ == '__main__':
    main()