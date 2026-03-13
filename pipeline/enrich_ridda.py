#!/usr/bin/env python3
"""
RIDDA WARS DATA ENRICHMENT v2.1
================================
Clean enrichment of primary source data + separate scholarly supplement.

Enrichment (added to primary data):
- Geographic coordinates
- Name normalization (EI3 transliteration)
- Cross-source event matching
- Region classification

Supplement (separate file):
- Campaign phases (Donner, Shoufani)
- Tribe profiles from scholarship
- Commander biographies
- Rebel leader profiles
- Battle descriptions
- Scholarly bibliography

Outputs:
- ridda_combined_enriched.json / .csv     (enriched primary data)
- ridda_combined_scholarly.json / .csv    (scholarly supplement)

Usage:
    python enrich_ridda.py --input ridda_combined.json
"""

import json
import csv
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import re

# ============================================================
# PART 1: ENRICHMENT DATA (Added to Primary Source)
# ============================================================

# Geographic coordinates for regions and battle sites - EXPANDED
COORDINATES = {
    # Yemen
    'اليمن': {'lat': 15.35, 'lon': 44.20, 'region': 'yaman', 'en': 'al-Yaman'},
    'صنعاء': {'lat': 15.35, 'lon': 44.21, 'region': 'yaman', 'en': 'Ṣanʿāʾ'},
    'تهامة': {'lat': 14.80, 'lon': 43.30, 'region': 'tihama', 'en': 'Tihāma'},
    'نجران': {'lat': 17.49, 'lon': 44.13, 'region': 'yaman', 'en': 'Najrān'},
    'مأرب': {'lat': 15.47, 'lon': 45.32, 'region': 'yaman', 'en': 'Maʾrib'},
    'أبين': {'lat': 13.05, 'lon': 45.37, 'region': 'yaman', 'en': 'Abyan'},
    'ظفر': {'lat': 14.20, 'lon': 44.40, 'region': 'yaman', 'en': 'Ẓafār'},
    
    # Hadramawt
    'حضرموت': {'lat': 15.95, 'lon': 48.78, 'region': 'hadramawt', 'en': 'Ḥaḍramawt'},
    'النجير': {'lat': 15.80, 'lon': 48.50, 'region': 'hadramawt', 'en': 'al-Nujayr'},
    
    # Najd - EXPANDED
    'نجد': {'lat': 25.00, 'lon': 45.00, 'region': 'najd', 'en': 'Najd'},
    'النجد': {'lat': 25.00, 'lon': 45.00, 'region': 'najd', 'en': 'Najd'},
    'بزاخة': {'lat': 27.50, 'lon': 43.50, 'region': 'najd', 'en': 'Buzākha'},
    'البزاخة': {'lat': 27.50, 'lon': 43.50, 'region': 'najd', 'en': 'Buzākha'},
    'سميراء': {'lat': 26.50, 'lon': 42.80, 'region': 'najd', 'en': 'Sumayra'},
    'الأبرق': {'lat': 25.00, 'lon': 42.50, 'region': 'najd', 'en': 'al-Abraq'},
    'البطاح': {'lat': 26.00, 'lon': 44.00, 'region': 'najd', 'en': 'al-Biṭāḥ'},
    'ذو القصة': {'lat': 24.80, 'lon': 40.50, 'region': 'najd', 'en': 'Dhū l-Qaṣṣa'},
    'البطاح والبعوضة': {'lat': 26.00, 'lon': 44.00, 'region': 'najd', 'en': 'al-Biṭāḥ wa-l-Baʿūḍa'},
    'الربذة': {'lat': 25.50, 'lon': 42.00, 'region': 'najd', 'en': 'al-Rabadha'},
    'جواء': {'lat': 26.20, 'lon': 43.80, 'region': 'najd', 'en': 'Jawwāʾ'},
    'الجواء': {'lat': 26.20, 'lon': 43.80, 'region': 'najd', 'en': 'Jawwāʾ'},
    'الغمر': {'lat': 26.80, 'lon': 43.20, 'region': 'najd', 'en': 'al-Ghamr'},
    'جو قراقر': {'lat': 25.50, 'lon': 44.50, 'region': 'najd', 'en': 'Jaww Qarāqir'},
    
    # al-Yamama
    'اليمامة': {'lat': 24.15, 'lon': 47.30, 'region': 'yamama', 'en': 'al-Yamāma'},
    'عقرباء': {'lat': 24.20, 'lon': 47.35, 'region': 'yamama', 'en': 'ʿAqrabāʾ'},
    'حجر': {'lat': 24.10, 'lon': 47.28, 'region': 'yamama', 'en': 'Ḥajr'},
    'حديقة الموت': {'lat': 24.18, 'lon': 47.32, 'region': 'yamama', 'en': 'Ḥadīqat al-Mawt'},
    
    # Bahrain
    'البحرين': {'lat': 26.00, 'lon': 50.55, 'region': 'bahrayn', 'en': 'al-Baḥrayn'},
    'هجر': {'lat': 25.38, 'lon': 49.60, 'region': 'bahrayn', 'en': 'Hajar'},
    'القطيف': {'lat': 26.56, 'lon': 50.01, 'region': 'bahrayn', 'en': 'al-Qaṭīf'},
    'جواثى': {'lat': 25.50, 'lon': 49.70, 'region': 'bahrayn', 'en': 'Jawāthā'},
    
    # Oman
    'عمان': {'lat': 23.58, 'lon': 58.38, 'region': 'uman', 'en': 'ʿUmān'},
    'دبا': {'lat': 25.62, 'lon': 56.27, 'region': 'uman', 'en': 'Dibā'},
    'صحار': {'lat': 24.36, 'lon': 56.75, 'region': 'uman', 'en': 'Ṣuḥār'},
    
    # Iraq - EXPANDED
    'العراق': {'lat': 33.00, 'lon': 44.00, 'region': 'iraq', 'en': 'al-ʿIrāq'},
    'الحيرة': {'lat': 31.98, 'lon': 44.45, 'region': 'iraq', 'en': 'al-Ḥīra'},
    'عين التمر': {'lat': 32.47, 'lon': 43.60, 'region': 'iraq', 'en': 'ʿAyn al-Tamr'},
    'الفراض': {'lat': 33.20, 'lon': 42.00, 'region': 'iraq', 'en': 'al-Firāḍ'},
    'العراق والجزيرة': {'lat': 33.50, 'lon': 43.00, 'region': 'iraq', 'en': 'al-ʿIrāq wa-l-Jazīra'},
    
    # Sham / Jazira - EXPANDED
    'الشام': {'lat': 33.50, 'lon': 36.30, 'region': 'sham', 'en': 'al-Shām'},
    'الجزيرة': {'lat': 36.00, 'lon': 41.00, 'region': 'jazira', 'en': 'al-Jazīra'},
    'دومة الجندل': {'lat': 29.81, 'lon': 39.87, 'region': 'sham', 'en': 'Dūmat al-Jandal'},
    
    # Hijaz - EXPANDED
    'المدينة': {'lat': 24.47, 'lon': 39.61, 'region': 'hijaz', 'en': 'al-Madīna'},
    'مكة': {'lat': 21.42, 'lon': 39.83, 'region': 'hijaz', 'en': 'Makka'},
    'جنوب طيبة': {'lat': 24.30, 'lon': 39.50, 'region': 'hijaz', 'en': 'South of Medina'},
    'حدود أرضهم': {'lat': 24.50, 'lon': 40.00, 'region': 'hijaz', 'en': 'Their territory borders'},
    
    # Tribal regions (approximate centers)
    'غطفان': {'lat': 26.00, 'lon': 42.00, 'region': 'najd', 'en': 'Ghaṭafān territory'},
    'قضاعة': {'lat': 28.00, 'lon': 37.00, 'region': 'sham', 'en': 'Quḍāʿa territory'},
    'الجزيرة العربية': {'lat': 24.00, 'lon': 45.00, 'region': 'arabia', 'en': 'Arabian Peninsula'},
}

# Name normalization (Arabic -> EI3 transliteration)
NAME_NORMALIZATION = {
    # Commanders
    'خالد بن الوليد': 'Khālid ibn al-Walīd',
    'خالد': 'Khālid ibn al-Walīd',
    'العلاء بن الحضرمي': 'al-ʿAlāʾ ibn al-Ḥaḍramī',
    'عكرمة بن أبي جهل': 'ʿIkrima ibn Abī Jahl',
    'عكرمة': 'ʿIkrima ibn Abī Jahl',
    'حذيفة بن محصن': 'Ḥudhayfa ibn Miḥṣan',
    'المهاجر بن أبي أمية': 'al-Muhājir ibn Abī Umayya',
    'شرحبيل بن حسنة': 'Shurahbīl ibn Ḥasana',
    'زياد بن لبيد': 'Ziyād ibn Labīd',
    'عامر بن شهر': 'ʿĀmir ibn Shahr',
    'عامر بن شهر الهمداني': 'ʿĀmir ibn Shahr al-Hamdānī',
    'أبو بكر': 'Abū Bakr al-Ṣiddīq',
    'فيروز الديلمي': 'Fayrūz al-Daylamī',
    'فيروز بن الديلمي': 'Fayrūz al-Daylamī',
    'قيس بن مكشوح': 'Qays ibn Makshūḥ',
    'قيس بن هبيرة المرادي': 'Qays ibn Hubayra al-Murādī',
    'يعلى بن منية': 'Yaʿlā ibn Munya',
    'سويد بن مقرن': 'Suwayd ibn Muqarrin',
    'عرفجة': 'ʿArfaja ibn Harthama',
    
    # Rebel leaders
    'مسيلمة': 'Musaylima ibn Ḥabīb',
    'مسيلمة الكذاب': 'Musaylima ibn Ḥabīb',
    'الأسود العنسي': 'al-Aswad al-ʿAnsī',
    'طليحة': 'Ṭulayḥa ibn Khuwaylid',
    'طليحة بن خويلد': 'Ṭulayḥa ibn Khuwaylid',
    'سجاح': 'Sajāḥ bint al-Ḥārith',
    'سجاح بنت أوس': 'Sajāḥ bint al-Ḥārith',
    'لقيط بن مالك': 'Laqīṭ ibn Mālik Dhū l-Tāj',
    'الأشعث بن قيس': 'al-Ashʿath ibn Qays',
    'مالك بن نويرة': 'Mālik ibn Nuwayra',
    'الحطم بن ضبيعة': 'al-Ḥuṭam ibn Ḍubayʿa',
    'عيينة بن حصن': 'ʿUyayna ibn Ḥiṣn',
    
    # Tribes
    'بنو حنيفة': 'Banū Ḥanīfa',
    'بنو أسد': 'Banū Asad',
    'بنو تميم': 'Banū Tamīm',
    'غطفان': 'Ghaṭafān',
    'بنو فزارة': 'Banū Fazāra',
    'طيء': 'Ṭayyiʾ',
    'بنو سليم': 'Banū Sulaym',
    'كندة': 'Kinda',
    'الأزد': 'al-Azd',
    'ربيعة': 'Rabīʿa',
    'عبس': 'ʿAbs',
    'ذبيان': 'Dhubyān',
    'بكر بن وائل': 'Bakr ibn Wāʾil',
    'عبد القيس': 'ʿAbd al-Qays',
    'همدان': 'Hamdān',
    'مذحج': 'Madhḥij',
    'عنس': 'ʿAns',
    'حمير': 'Ḥimyar',
    'خولان': 'Khawlān',
    'مراد': 'Murād',
    'النخع': 'al-Nakhaʿ',
    'بجيلة': 'Bajīla',
    'خثعم': 'Khathʿam',
    'بنو عامر': 'Banū ʿĀmir',
    'هوازن': 'Hawāzin',
    'ثقيف': 'Thaqīf',
    'قضاعة': 'Quḍāʿa',
    'كلب': 'Kalb',
    'بهراء': 'Bahrāʾ',
}

# Region display names
REGION_NAMES = {
    'yaman': {'ar': 'اليمن', 'en': 'al-Yaman (Yemen)'},
    'tihama': {'ar': 'تهامة', 'en': 'Tihāma'},
    'hadramawt': {'ar': 'حضرموت', 'en': 'Ḥaḍramawt'},
    'najd': {'ar': 'نجد', 'en': 'Najd (Central Arabia)'},
    'yamama': {'ar': 'اليمامة', 'en': 'al-Yamāma'},
    'bahrayn': {'ar': 'البحرين', 'en': 'al-Baḥrayn'},
    'uman': {'ar': 'عمان', 'en': 'ʿUmān (Oman)'},
    'iraq': {'ar': 'العراق', 'en': 'al-ʿIrāq'},
    'sham': {'ar': 'الشام', 'en': 'al-Shām'},
    'hijaz': {'ar': 'الحجاز', 'en': 'al-Ḥijāz'},
    'jazira': {'ar': 'الجزيرة', 'en': 'al-Jazīra'},
    'arabia': {'ar': 'الجزيرة العربية', 'en': 'Arabian Peninsula'},
}


# ============================================================
# PART 2: SCHOLARLY SUPPLEMENT DATA
# ============================================================

SCHOLARLY_SUPPLEMENT = {
    "metadata": {
        "title": "Ridda Wars Scholarly Supplement",
        "title_ar": "ملحق علمي لحروب الردة",
        "description": "Scholarly apparatus for the Ridda Wars Database, compiled from Western academic sources",
        "version": "1.0",
        "created": datetime.now().strftime("%Y-%m-%d"),
        "note": "This supplement provides scholarly context separate from the primary source extraction"
    },
    
    "bibliography": {
        "primary_sources": {
            "tabari": {
                "author": "Muḥammad ibn Jarīr al-Ṭabarī",
                "author_ar": "محمد بن جرير الطبري",
                "death": "310 AH / 923 CE",
                "work": "Taʾrīkh al-Rusul wa-l-Mulūk",
                "work_ar": "تاريخ الرسل والملوك",
                "translation": "The History of al-Ṭabarī, vol. 10: The Conquest of Arabia, trans. F. Donner (Albany, 1993)",
                "genre": "Universal chronicle (annalistic)",
                "ridda_coverage": "Extensive, organized by year (11-12 AH)"
            },
            "baladhuri": {
                "author": "Aḥmad ibn Yaḥyā al-Balādhurī",
                "author_ar": "أحمد بن يحيى البلاذري",
                "death": "279 AH / 892 CE",
                "work": "Futūḥ al-Buldān",
                "work_ar": "فتوح البلدان",
                "translation": "The Origins of the Islamic State, trans. P.K. Hitti & F.C. Murgotten (New York, 1916-24)",
                "genre": "Conquest geography (topographical)",
                "ridda_coverage": "Regional focus, organized by location"
            },
            "ibn_ishaq": {
                "author": "Muḥammad ibn Isḥāq",
                "author_ar": "محمد بن إسحاق",
                "death": "150 AH / 767 CE",
                "work": "al-Sīra al-Nabawiyya (via Ibn Hishām)",
                "work_ar": "السيرة النبوية",
                "note": "Earliest biographical source, limited Ridda material"
            },
            "waqidi": {
                "author": "Muḥammad ibn ʿUmar al-Wāqidī",
                "author_ar": "محمد بن عمر الواقدي",
                "death": "207 AH / 823 CE",
                "work": "Kitāb al-Ridda",
                "work_ar": "كتاب الردة",
                "note": "Lost work, fragments preserved in later sources"
            }
        },
        
        "secondary_sources": {
            "donner_1981": {
                "author": "Fred McGraw Donner",
                "title": "The Early Islamic Conquests",
                "publisher": "Princeton University Press",
                "year": 1981,
                "pages": "82-162 (Chapter 3: The Riddah Wars)",
                "significance": "Standard modern analysis; emphasizes political-economic factors over religious",
                "key_arguments": [
                    "Ridda was primarily political secession, not religious apostasy",
                    "Tribes sought to break tax obligations after Prophet's death",
                    "Abū Bakr's campaigns established caliphal authority over Arabia",
                    "Military success enabled subsequent expansion into Iraq/Syria"
                ],
                "isbn": "978-0691053271"
            },
            "shoufani_1972": {
                "author": "Elias Shoufani",
                "title": "Al-Riddah and the Muslim Conquest of Arabia",
                "publisher": "University of Toronto Press",
                "year": 1972,
                "significance": "First dedicated monograph on Ridda Wars",
                "key_arguments": [
                    "Distinguishes between true apostasy and political rebellion",
                    "Emphasizes tribal factionalism in understanding events",
                    "Detailed analysis of false prophets phenomenon"
                ],
                "isbn": "978-0802018175"
            },
            "landau_tasseron_1990": {
                "author": "Ella Landau-Tasseron",
                "title": "The Sinful Wars: Religious, Social and Historical Aspects of Ḥurūb al-Ridda",
                "journal": "Jerusalem Studies in Arabic and Islam",
                "volume": 14,
                "year": 1990,
                "pages": "37-54",
                "significance": "Analyzes religious dimensions of Ridda",
                "key_arguments": [
                    "Term 'ridda' carried strong religious connotations",
                    "Sources consciously shaped narrative for religious purposes"
                ]
            },
            "landau_tasseron_1995": {
                "author": "Ella Landau-Tasseron",
                "title": "Features of the Pre-Conquest Muslim Army in the Time of Muḥammad",
                "in": "The Byzantine and Early Islamic Near East III: States, Resources and Armies",
                "editor": "Averil Cameron",
                "publisher": "Darwin Press",
                "year": 1995,
                "pages": "299-336",
                "significance": "Military organization during Ridda campaigns"
            },
            "lecker_1989": {
                "author": "Michael Lecker",
                "title": "The Death of the Prophet Muḥammad's Father: Did Wāqidī Invent Some of the Evidence?",
                "journal": "Zeitschrift der Deutschen Morgenländischen Gesellschaft",
                "volume": 145,
                "year": 1995,
                "significance": "Source criticism methodology applicable to Ridda sources"
            },
            "lecker_2005": {
                "author": "Michael Lecker",
                "title": "Tribes and Clans in Early Arabia",
                "in": "People, Tribes and Society in Arabia Around the Time of Muḥammad",
                "publisher": "Ashgate",
                "year": 2005,
                "significance": "Tribal structures essential for understanding Ridda"
            },
            "kister_1986": {
                "author": "Meir Jacob Kister",
                "title": "The Struggle against Musaylima and the Conquest of Yamāma",
                "journal": "Jerusalem Studies in Arabic and Islam",
                "volume": 8,
                "year": 1986,
                "pages": "1-47",
                "significance": "Definitive study of Yamāma campaign",
                "key_arguments": [
                    "Detailed reconstruction of Battle of ʿAqrabāʾ",
                    "Analysis of Musaylima's movement",
                    "Casualties and their impact on early Islam"
                ]
            },
            "kennedy_2007": {
                "author": "Hugh Kennedy",
                "title": "The Great Arab Conquests",
                "publisher": "Da Capo Press",
                "year": 2007,
                "pages": "50-73",
                "significance": "Accessible overview, good maps",
                "isbn": "978-0306817403"
            },
            "hoyland_2015": {
                "author": "Robert G. Hoyland",
                "title": "In God's Path: The Arab Conquests and the Creation of an Islamic Empire",
                "publisher": "Oxford University Press",
                "year": 2015,
                "significance": "Recent synthesis incorporating non-Arabic sources",
                "isbn": "978-0199916368"
            },
            "powers_1989": {
                "author": "David S. Powers",
                "title": "On the Abrogation of the Bequest Verses",
                "journal": "Arabica",
                "volume": 29,
                "year": 1982,
                "note": "Relevant to succession crisis triggering Ridda"
            },
            "crone_1987": {
                "author": "Patricia Crone",
                "title": "Meccan Trade and the Rise of Islam",
                "publisher": "Princeton University Press",
                "year": 1987,
                "note": "Economic context of Arabian tribes"
            }
        },
        
        "encyclopedias": {
            "ei2": {
                "title": "Encyclopaedia of Islam, Second Edition",
                "publisher": "Brill",
                "years": "1960-2005",
                "relevant_articles": [
                    "Ridda",
                    "Musaylima",
                    "al-Aswad al-ʿAnsī",
                    "Ṭulayḥa",
                    "Sadjāḥ",
                    "Khālid b. al-Walīd",
                    "Abū Bakr",
                    "Ḥanīfa (Banū)",
                    "Yamāma",
                    "Kinda"
                ]
            },
            "ei3": {
                "title": "Encyclopaedia of Islam, Third Edition",
                "publisher": "Brill",
                "years": "2007-present",
                "note": "Updated articles with recent scholarship"
            }
        }
    },
    
    "campaign_phases": {
        "description": "Analytical framework based on Donner (1981) and Shoufani (1972)",
        "note": "These phases are scholarly constructs, not categories from primary sources",
        "phases": {
            "phase_0": {
                "name": "Succession Crisis",
                "name_ar": "أزمة الخلافة",
                "date": "11 AH (June 632 CE)",
                "duration": "Days",
                "description": "Death of Prophet Muḥammad and election of Abū Bakr",
                "key_events": [
                    "Prophet dies 12 Rabīʿ I 11 AH (8 June 632)",
                    "Saqīfa meeting - Abū Bakr elected caliph",
                    "News spreads to Arabian tribes"
                ],
                "sources": ["Donner 1981: 82-85"]
            },
            "phase_1": {
                "name": "Yemen Campaign",
                "name_ar": "حملة اليمن",
                "date": "Late 10 - Early 11 AH",
                "duration": "~3 months",
                "description": "Suppression of al-Aswad al-ʿAnsī's movement in Yemen",
                "regions": ["yaman", "tihama"],
                "key_events": [
                    "al-Aswad claims prophethood (late 10 AH)",
                    "al-Aswad conquers Yemen, kills Persian governor Bādhām's son",
                    "Assassination plot by Qays ibn Makshūḥ and Fayrūz al-Daylamī",
                    "al-Aswad killed in Ṣanʿāʾ (night before Prophet's death or shortly after)"
                ],
                "commanders": ["Qays ibn Makshūḥ", "Fayrūz al-Daylamī", "Local Yemeni chiefs"],
                "opponents": ["al-Aswad al-ʿAnsī", "ʿAns tribe"],
                "outcome": "al-Aswad assassinated; Yemen returned to Islamic authority",
                "casualties": "Unknown, assassination was covert operation",
                "sources": ["Donner 1981: 86-91", "Shoufani 1972: 71-89", "al-Ṭabarī I: 1845-1866"]
            },
            "phase_2": {
                "name": "Defense of Medina",
                "name_ar": "الدفاع عن المدينة",
                "date": "11 AH (July 632)",
                "duration": "~2 weeks",
                "description": "Initial defense against tribes threatening Medina",
                "regions": ["hijaz", "najd"],
                "key_events": [
                    "Tribal delegations demand return of ṣadaqa (alms)",
                    "Abū Bakr refuses: 'By God, if they withhold a she-kid...'",
                    "Bedouin tribes gather at Dhū l-Qaṣṣa",
                    "Abū Bakr leads defense, defeats raiders at Dhū l-Qaṣṣa"
                ],
                "commanders": ["Abū Bakr (personally)"],
                "opponents": ["ʿAbs", "Dhubyān", "elements of Asad and Ghaṭafān"],
                "outcome": "Immediate threat to Medina repelled",
                "sources": ["Donner 1981: 91-95"]
            },
            "phase_3": {
                "name": "Najd Campaign (Buzākha)",
                "name_ar": "حملة نجد - بزاخة",
                "date": "11 AH (August-September 632)",
                "duration": "~6 weeks",
                "description": "Campaign against Ṭulayḥa and allied tribes in central Arabia",
                "regions": ["najd"],
                "key_events": [
                    "Khālid ibn al-Walīd appointed commander",
                    "Battle of Buzākha - Asad and Ghaṭafān defeated",
                    "Ṭulayḥa flees to Syria (later converts back to Islam)",
                    "ʿUyayna ibn Ḥiṣn captured then released"
                ],
                "commanders": ["Khālid ibn al-Walīd"],
                "opponents": ["Ṭulayḥa ibn Khuwaylid", "Banū Asad", "Ghaṭafān", "Banū Fazāra"],
                "outcome": "Decisive Muslim victory; Ṭulayḥa's movement collapses",
                "casualties": "Significant on both sides",
                "sources": ["Donner 1981: 101-110", "Shoufani 1972: 89-100"]
            },
            "phase_4": {
                "name": "Tamīm Campaign (al-Biṭāḥ)",
                "name_ar": "حملة تميم - البطاح",
                "date": "11 AH (September-October 632)",
                "duration": "~4 weeks",
                "description": "Controversial campaign against Banū Tamīm",
                "regions": ["najd"],
                "key_events": [
                    "Khālid moves against Banū Tamīm elements",
                    "Mālik ibn Nuwayra refuses to pay zakāt",
                    "Execution of Mālik ibn Nuwayra (controversial)",
                    "Khālid marries Mālik's widow (scandal)",
                    "ʿUmar demands Khālid's punishment; Abū Bakr refuses"
                ],
                "commanders": ["Khālid ibn al-Walīd"],
                "opponents": ["Mālik ibn Nuwayra", "elements of Banū Tamīm"],
                "outcome": "Tamīm pacified amid controversy",
                "controversy": "Mālik's execution debated - was he Muslim or apostate?",
                "sources": ["Donner 1981: 110-115", "Lecker on Mālik controversy"]
            },
            "phase_5": {
                "name": "Yamāma Campaign",
                "name_ar": "حملة اليمامة",
                "date": "11-12 AH (Late 632 - Early 633)",
                "duration": "~2-3 months",
                "description": "Major campaign against Musaylima and Banū Ḥanīfa",
                "regions": ["yamama"],
                "key_events": [
                    "Initial Muslim defeat at ʿAqrabāʾ (first engagement)",
                    "Reinforcements arrive under Khālid",
                    "Battle of ʿAqrabāʾ (main battle)",
                    "Garden of Death (Ḥadīqat al-Mawt) - final stand",
                    "Musaylima killed by Waḥshī ibn Ḥarb",
                    "Heavy casualties among ḥuffāẓ (Quran memorizers)"
                ],
                "commanders": ["Khālid ibn al-Walīd", "Shurahbīl ibn Ḥasana", "ʿIkrima ibn Abī Jahl (initial)"],
                "opponents": ["Musaylima ibn Ḥabīb", "Banū Ḥanīfa", "Mujjāʿa ibn Murāra"],
                "outcome": "Decisive Muslim victory; Musaylima killed; Ḥanīfa submit",
                "casualties": "~1,200 Muslims killed (including many ḥuffāẓ); ~10,000+ Ḥanīfa killed",
                "significance": "Largest and most important battle of Ridda Wars",
                "sources": ["Donner 1981: 115-128", "Kister 1986 (detailed study)", "Shoufani 1972: 100-130"]
            },
            "phase_6": {
                "name": "Bahrain Campaign",
                "name_ar": "حملة البحرين",
                "date": "11-12 AH",
                "duration": "Several months",
                "description": "Suppression of rebellion in eastern Arabia",
                "regions": ["bahrayn"],
                "key_events": [
                    "al-Ḥuṭam ibn Ḍubayʿa leads rebellion",
                    "Siege of Jawāthā",
                    "al-ʿAlāʾ ibn al-Ḥaḍramī's campaign",
                    "ʿAbd al-Qays remain loyal to Islam",
                    "Bakr ibn Wāʾil elements rebel then submit"
                ],
                "commanders": ["al-ʿAlāʾ ibn al-Ḥaḍramī"],
                "opponents": ["al-Ḥuṭam ibn Ḍubayʿa", "elements of Bakr ibn Wāʾil"],
                "outcome": "Bahrain pacified",
                "sources": ["Donner 1981: 128-135"]
            },
            "phase_7": {
                "name": "Oman Campaign",
                "name_ar": "حملة عمان",
                "date": "11-12 AH",
                "duration": "Several months",
                "description": "Campaign against Laqīṭ in Oman",
                "regions": ["uman"],
                "key_events": [
                    "Laqīṭ ibn Mālik Dhū l-Tāj claims kingship",
                    "Battle of Dibā",
                    "Laqīṭ killed",
                    "Azd of Oman submit"
                ],
                "commanders": ["Ḥudhayfa ibn Miḥṣan", "ʿIkrima ibn Abī Jahl"],
                "opponents": ["Laqīṭ ibn Mālik Dhū l-Tāj", "Azd of Oman"],
                "outcome": "Oman pacified; Laqīṭ killed",
                "sources": ["Donner 1981: 135-140"]
            },
            "phase_8": {
                "name": "Ḥaḍramawt Campaign",
                "name_ar": "حملة حضرموت",
                "date": "12 AH",
                "duration": "Several months",
                "description": "Final campaign against Kinda in Ḥaḍramawt",
                "regions": ["hadramawt"],
                "key_events": [
                    "Kinda under al-Ashʿath ibn Qays rebel",
                    "Siege of al-Nujayr fortress",
                    "al-Ashʿath captured attempting to flee",
                    "al-Ashʿath pardoned, marries Abū Bakr's sister",
                    "al-Ashʿath later becomes Muslim commander"
                ],
                "commanders": ["Ziyād ibn Labīd", "al-Muhājir ibn Abī Umayya"],
                "opponents": ["al-Ashʿath ibn Qays", "Kinda"],
                "outcome": "Kinda submit; al-Ashʿath rehabilitated",
                "sources": ["Donner 1981: 140-145"]
            }
        }
    },
    
    "biographical_profiles": {
        "false_prophets": {
            "musaylima": {
                "name_ar": "مسيلمة بن حبيب",
                "name_ei3": "Musaylima ibn Ḥabīb",
                "epithet": "al-Kadhdhāb (الكذاب) - the Liar",
                "tribe": "Banū Ḥanīfa (Rabīʿa confederation)",
                "region": "al-Yamāma",
                "claim": "Prophet receiving revelation from al-Raḥmān",
                "teachings": [
                    "Claimed verses/revelations (some preserved in sources)",
                    "Reduced prayer requirements",
                    "Permitted wine",
                    "Taught form of monotheism"
                ],
                "political_actions": [
                    "Wrote to Prophet claiming shared prophethood",
                    "Controlled al-Yamāma after Prophet's death",
                    "Married Sajāḥ (alliance)",
                    "Built army of ~40,000"
                ],
                "death": "Killed at Battle of ʿAqrabāʾ by Waḥshī ibn Ḥarb (12 AH)",
                "sources": ["Kister 1986", "EI2: Musaylima"],
                "scholarly_notes": "Most serious threat to nascent Islamic state; his movement represented alternative Arabian monotheism"
            },
            "aswad_ansi": {
                "name_ar": "الأسود العنسي (عبهلة بن كعب)",
                "name_ei3": "al-Aswad al-ʿAnsī (ʿAbhala ibn Kaʿb)",
                "epithet": "Dhū l-Khimār (ذو الخمار) - the Veiled One",
                "tribe": "ʿAns (Madhḥij confederation)",
                "region": "Yemen",
                "claim": "Prophet; claimed angel visited him",
                "teachings": [
                    "Claimed divine inspiration",
                    "Used sorcery/magic (according to sources)",
                    "Veiled face (hence epithet)"
                ],
                "political_actions": [
                    "Conquered much of Yemen",
                    "Killed Bādhām's son (Persian governor)",
                    "Married Bādhām's daughter",
                    "Controlled Ṣanʿāʾ"
                ],
                "death": "Assassinated in Ṣanʿāʾ by Qays ibn Makshūḥ and Fayrūz al-Daylamī (11 AH)",
                "sources": ["Donner 1981: 86-91", "EI2: al-Aswad"],
                "scholarly_notes": "First false prophet chronologically; killed just before/after Prophet's death"
            },
            "tulayha": {
                "name_ar": "طليحة بن خويلد الأسدي",
                "name_ei3": "Ṭulayḥa ibn Khuwaylid al-Asadī",
                "epithet": None,
                "tribe": "Banū Asad",
                "region": "Najd",
                "claim": "Prophet receiving revelation from Dhū l-Nūn",
                "teachings": "Little preserved; claimed ongoing revelation",
                "political_actions": [
                    "United Asad and Ghaṭafān tribes",
                    "Allied with ʿUyayna ibn Ḥiṣn (Fazāra chief)",
                    "Confronted Muslim army at Buzākha"
                ],
                "death": "Survived - fled to Syria, later converted back to Islam, died at Nihāwand (21 AH) fighting as Muslim",
                "sources": ["Donner 1981: 101-110", "EI2: Ṭulayḥa"],
                "scholarly_notes": "Unlike others, survived and rehabilitated; later Muslim commander"
            },
            "sajah": {
                "name_ar": "سجاح بنت الحارث التميمية",
                "name_ei3": "Sajāḥ bint al-Ḥārith",
                "epithet": None,
                "tribe": "Banū Tamīm / Banū Taghlib (Christian tribe)",
                "region": "Jazīra / Najd",
                "claim": "Prophetess",
                "teachings": "Little preserved; may have had Christian influences",
                "political_actions": [
                    "Led force from Jazīra into Arabia",
                    "Allied with then married Musaylima",
                    "Withdrew after Musaylima's death"
                ],
                "death": "Survived - converted to Islam, died during Muʿāwiya's caliphate",
                "sources": ["Shoufani 1972: 110-115", "EI2: Sadjāḥ"],
                "scholarly_notes": "Only female claimant; Christian tribal background significant"
            },
            "laqit": {
                "name_ar": "لقيط بن مالك ذو التاج",
                "name_ei3": "Laqīṭ ibn Mālik Dhū l-Tāj",
                "epithet": "Dhū l-Tāj (ذو التاج) - the Crowned One",
                "tribe": "al-Azd",
                "region": "Oman",
                "claim": "King (not prophet) - restored pre-Islamic kingship",
                "political_actions": [
                    "Claimed kingship in Oman",
                    "Led Azd resistance",
                    "Confronted Muslim forces at Dibā"
                ],
                "death": "Killed at Battle of Dibā (11-12 AH)",
                "sources": ["Donner 1981: 135-140", "EI2: Laḳīṭ"],
                "scholarly_notes": "Different from false prophets - claimed royal not prophetic authority"
            }
        },
        
        "commanders": {
            "khalid_ibn_walid": {
                "name_ar": "خالد بن الوليد المخزومي",
                "name_ei3": "Khālid ibn al-Walīd",
                "epithet": "Sayf Allāh (سيف الله) - Sword of God",
                "tribe": "Quraysh (Banū Makhzūm)",
                "birth": "c. 585 CE",
                "death": "642 CE (21 AH) in Homs",
                "pre_islamic": "Meccan cavalry commander; led charge at Uḥud against Muslims",
                "conversion": "c. 629 CE (8 AH)",
                "ridda_commands": [
                    "Supreme commander of main army",
                    "Buzākha campaign (vs. Ṭulayḥa)",
                    "al-Biṭāḥ campaign (vs. Tamīm)",
                    "al-Yamāma campaign (vs. Musaylima)"
                ],
                "controversies": [
                    "Execution of Mālik ibn Nuwayra",
                    "Marriage to Mālik's widow",
                    "ʿUmar demanded his punishment"
                ],
                "later_career": "Conquered Iraq and Syria; dismissed by ʿUmar (17 AH)",
                "sources": ["EI2: Khālid b. al-Walīd", "Donner 1981 passim"],
                "assessment": "Most successful Arab military commander; crucial to Ridda and Futūḥāt success"
            },
            "ala_ibn_hadrami": {
                "name_ar": "العلاء بن الحضرمي",
                "name_ei3": "al-ʿAlāʾ ibn al-Ḥaḍramī",
                "tribe": "Ḥaḍramawt",
                "death": "21 AH (642 CE)",
                "ridda_commands": ["Bahrain campaign"],
                "later_career": "Governor of Bahrain; unauthorized attack on Persia",
                "sources": ["EI2: al-ʿAlāʾ b. al-Ḥaḍramī"]
            },
            "ikrima": {
                "name_ar": "عكرمة بن أبي جهل",
                "name_ei3": "ʿIkrima ibn Abī Jahl",
                "tribe": "Quraysh (Banū Makhzūm)",
                "father": "Abū Jahl (major Meccan opponent of Islam)",
                "conversion": "630 CE (conquest of Mecca)",
                "death": "13 AH (634 CE) at Battle of Yarmūk",
                "ridda_commands": [
                    "Initial Yamāma force (unsuccessful)",
                    "Oman campaign (with Ḥudhayfa)",
                    "Mahra campaign"
                ],
                "sources": ["EI2: ʿIkrima b. Abī Djahl"],
                "note": "Son of Islam's greatest enemy became Muslim commander"
            }
        },
        
        "tribal_leaders": {
            "malik_ibn_nuwayra": {
                "name_ar": "مالك بن نويرة اليربوعي",
                "name_ei3": "Mālik ibn Nuwayra",
                "tribe": "Banū Yarbūʿ (Tamīm)",
                "role": "Tax collector (ʿāmil) for Prophet",
                "actions": [
                    "Distributed ṣadaqa back to tribe after Prophet's death",
                    "Refused to pay to Abū Bakr",
                    "Allegedly recited poetry questioning succession"
                ],
                "death": "Executed by Khālid's forces at al-Biṭāḥ (11 AH)",
                "controversy": [
                    "Was he Muslim or apostate?",
                    "Executed after claiming shahāda",
                    "Brother Mutammim's elegies preserve his memory",
                    "Khālid married his widow same night"
                ],
                "sources": ["Donner 1981: 110-115", "Various articles on controversy"],
                "scholarly_notes": "Most controversial incident of Ridda Wars; debated whether he was truly apostate"
            },
            "ashath_ibn_qays": {
                "name_ar": "الأشعث بن قيس الكندي",
                "name_ei3": "al-Ashʿath ibn Qays",
                "tribe": "Kinda",
                "role": "Chief of Kinda; led Ḥaḍramawt rebellion",
                "actions": [
                    "Led Kinda rebellion after Prophet's death",
                    "Besieged at al-Nujayr fortress",
                    "Attempted escape, captured"
                ],
                "outcome": [
                    "Pardoned by Abū Bakr",
                    "Married Abū Bakr's sister",
                    "Became Muslim commander",
                    "Fought at Yarmūk and Qādisiyya",
                    "Involved in later fitna politics"
                ],
                "death": "40 AH",
                "sources": ["EI2: al-Ashʿath", "Donner 1981: 140-145"]
            },
            "uyayna_ibn_hisn": {
                "name_ar": "عيينة بن حصن الفزاري",
                "name_ei3": "ʿUyayna ibn Ḥiṣn",
                "tribe": "Banū Fazāra (Ghaṭafān)",
                "role": "Chief of Fazāra",
                "actions": [
                    "Allied with Ṭulayḥa",
                    "Fought at Buzākha",
                    "Captured by Muslims"
                ],
                "outcome": "Pardoned, brought to Medina, released",
                "sources": ["Donner 1981: 101-110"],
                "note": "Example of Abū Bakr's policy of reconciliation"
            }
        }
    },
    
    "tribal_data": {
        "description": "Comprehensive tribal database for Ridda Wars with genealogical, geographic, and historical data",
        "sources": ["EI2", "EI3", "Donner 1981", "Shoufani 1972", "Kister 1986", "al-Ṭabarī", "al-Balādhurī"],
        "confederations": {
            "adnan": {
                "name_ei3": "ʿAdnān",
                "type": "Northern Arabian",
                "description": "Northern Arabian confederation claiming descent from Ismāʿīl via ʿAdnān",
                "genealogy": "ʿAdnān ← Maʿadd ← Nizār branches into Muḍar and Rabīʿa",
                "major_groups": ["Muḍar", "Rabīʿa"],
                "territory": "Central and Northern Arabia (Najd, al-Yamāma, al-Baḥrayn, borders of Iraq)",
                "ridda_tribes": ["Banū Ḥanīfa", "Banū Asad", "Banū Tamīm", "Ghaṭafān", "Banū Sulaym", "Bakr ibn Wāʾil", "Banū ʿĀmir"],
                "characteristics": "Generally pastoral nomads; some agricultural settlements"
            },
            "qahtan": {
                "name_ei3": "Qaḥṭān",
                "type": "Southern Arabian",
                "description": "Southern Arabian confederation claiming descent from Qaḥṭān (biblical Joktan)",
                "genealogy": "Qaḥṭān → branches into Ḥimyar and Kahlān",
                "major_groups": ["Ḥimyar", "Kahlān"],
                "territory": "Yemen, Ḥaḍramawt, Oman, scattered settlements northward",
                "ridda_tribes": ["Kinda", "Azd", "Madhḥij", "Ḥimyar", "Ṭayyiʾ"],
                "characteristics": "Mix of sedentary (Yemen) and pastoral populations"
            }
        },
        "tribes": {
            "banu_hanifa": {
                "name_ei3": "Banū Ḥanīfa",
                "confederation": "ʿAdnān (Rabīʿa branch)",
                "genealogy": "Ḥanīfa → Lujaym → Ṣaʿb → ʿAlī → Bakr → Wāʾil → Rabīʿa",
                "region": "al-Yamāma",
                "main_settlement": "Ḥajr (later al-Yamāma, now al-Riyadh area)",
                "economy": "Agriculture (date palms, grains), trade routes",
                "population": "Large - dominant tribe of al-Yamāma; ~40,000 warriors claimed",
                "pre_islamic": "Pagan; some Christian influence in region",
                "ridda_role": "Main opposition - followed Musaylima's prophetic movement",
                "ridda_leader": "Musaylima ibn Ḥabīb (al-Kadhdhāb)",
                "leader_claim": "Prophet receiving waḥy (revelation); composed rival 'Quran'",
                "military_strength": "Largest rebel force; well-organized agricultural base",
                "key_battle": "ʿAqrabāʾ (12 AH)",
                "battle_details": "Fierce resistance; 'Garden of Death' last stand",
                "outcome": "Defeated after bloodiest battle of Ridda Wars",
                "casualties": "Heavy on both sides; ~700 Muslim ḥuffāẓ killed",
                "aftermath": "Submitted; many joined futūḥāt; tribe remained in Yamāma",
                "notable_members": ["Musaylima ibn Ḥabīb", "Mujjāʿa ibn Murāra", "Rajjāl ibn ʿUnfuwa"],
                "sources": ["EI2: Ḥanīfa", "Kister 1986: Struggle against Musaylima", "Donner 1981: Ch. 4"],
                "scholarly_note": "Represents most serious alternative to Islam in Arabia"
            },
            "banu_asad": {
                "name_ei3": "Banū Asad",
                "confederation": "ʿAdnān (Muḍar branch, Khindif)",
                "genealogy": "Asad → Khuzayma → Mudrika → Ilyās → Muḍar → Nizār → ʿAdnān",
                "region": "Najd (northern)",
                "territory": "Between Ṭayyiʾ mountains and al-Yamāma",
                "economy": "Pastoralism, raiding",
                "pre_islamic": "Mostly pagan; some interaction with Christians",
                "ridda_role": "Followed Ṭulayḥa ibn Khuwaylid's prophetic movement",
                "ridda_leader": "Ṭulayḥa ibn Khuwaylid al-Asadī",
                "leader_claim": "Prophet receiving revelation from Dhū l-Nūn",
                "alliance": "Allied with Ghaṭafān, elements of Ṭayyiʾ",
                "key_battle": "Buzākha (11 AH)",
                "outcome": "Defeated; Ṭulayḥa fled to Syria",
                "aftermath": "Tribe submitted; Ṭulayḥa later converted, died at Nihāwand as Muslim",
                "notable_members": ["Ṭulayḥa ibn Khuwaylid", "Ḍirār ibn al-Azwar"],
                "sources": ["EI2: Asad", "Donner 1981: Ch. 3"]
            },
            "banu_tamim": {
                "name_ei3": "Banū Tamīm",
                "confederation": "ʿAdnān (Muḍar branch)",
                "genealogy": "Tamīm → Murr → Udd → Ṭābikha → Ilyās → Muḍar → Nizār",
                "region": "Najd (central) / al-Yamāma borders",
                "territory": "al-Dahna desert and surrounding areas",
                "economy": "Pastoralism, some agriculture",
                "population": "Very large - one of the largest Arabian tribes",
                "pre_islamic": "Mostly pagan; noted for poetry and oratory",
                "ridda_role": "Divided - some withheld zakāt, some followed Sajāḥ",
                "ridda_leaders": ["Mālik ibn Nuwayra (zakāt refusal)", "Sajāḥ bint al-Ḥārith (prophetess)"],
                "internal_division": "Yarbūʿ branch under Mālik; others under various chiefs",
                "sub_tribes": ["Banū Yarbūʿ", "Banū Mujāshiʿ", "Banū Saʿd", "Banū Ḥanẓala", "Banū ʿAmr"],
                "key_event": "Execution of Mālik ibn Nuwayra by Khālid at al-Buṭāḥ",
                "controversy": "Mālik's status as rebel vs. loyal Muslim hotly debated",
                "outcome": "Pacified; Mālik executed (controversial)",
                "aftermath": "ʿUmar investigated Khālid; Tamīm important in futūḥāt",
                "notable_members": ["Mālik ibn Nuwayra", "Sajāḥ bint al-Ḥārith", "al-Aqraʿ ibn Ḥābis", "al-Aḥnaf ibn Qays", "al-Qaʿqāʿ ibn ʿAmr"],
                "sources": ["EI2: Tamīm", "Donner 1981: 110-115"],
                "scholarly_note": "Mālik case is most controversial incident of Ridda Wars"
            },
            "ghatafan": {
                "name_ei3": "Ghaṭafān",
                "confederation": "ʿAdnān (Qays ʿAylān branch, Muḍar)",
                "genealogy": "Ghaṭafān → Saʿd → Qays ʿAylān → Muḍar → Nizār",
                "region": "Najd (western/northern)",
                "territory": "Between Khaybar and Fadak; north of Medina",
                "economy": "Pastoralism, raiding, some agriculture at oases",
                "pre_islamic": "Hostile to Prophet; participated in siege of Medina (Khandaq)",
                "ridda_role": "Allied with Ṭulayḥa and Banū Asad",
                "ridda_leader": "ʿUyayna ibn Ḥiṣn al-Fazārī (chief of Fazāra)",
                "sub_tribes": ["Fazāra", "ʿAbs", "Dhubyān", "Ashjaʿ"],
                "key_battle": "Buzākha (11 AH)",
                "outcome": "Defeated alongside Banū Asad",
                "aftermath": "Submitted; ʿUyayna captured then pardoned by Abū Bakr",
                "notable_members": ["ʿUyayna ibn Ḥiṣn", "al-Ḥārith ibn ʿAwf"],
                "sources": ["EI2: Ghaṭafān", "Donner 1981: 101-110"],
                "pre_islamic_note": "Previously major opponents of Islam"
            },
            "banu_sulaym": {
                "name_ei3": "Banū Sulaym",
                "confederation": "ʿAdnān (Qays ʿAylān branch, Muḍar)",
                "genealogy": "Sulaym → Manṣūr → ʿIkrima → Qays ʿAylān → Muḍar",
                "region": "Najd (western) / Ḥijāz borders",
                "territory": "East of Medina toward Najd",
                "pre_islamic": "Some early converts; participated in later expeditions",
                "ridda_role": "Brief wavering; returned quickly without major conflict",
                "outcome": "Submitted without major fighting",
                "aftermath": "Remained loyal; major participants in futūḥāt (especially North Africa)",
                "sources": ["EI2: Sulaym"]
            },
            "banu_amir": {
                "name_ei3": "Banū ʿĀmir ibn Ṣaʿṣaʿa",
                "confederation": "ʿAdnān (Hawāzin branch)",
                "genealogy": "ʿĀmir → Ṣaʿṣaʿa → Muʿāwiya → Bakr → Hawāzin",
                "region": "Najd (southern)",
                "territory": "Between Najd and al-Yamāma",
                "population": "Large confederation",
                "ridda_role": "Divided - some sections rebelled, others remained loyal",
                "sub_tribes": ["Banū Kilāb", "Banū Numayr", "Banū Hilāl", "Banū Kaʿb"],
                "outcome": "Mixed - some fighting, some peaceful submission",
                "sources": ["EI2: ʿĀmir b. Ṣaʿṣaʿa"]
            },
            "kinda": {
                "name_ei3": "Kinda",
                "confederation": "Qaḥṭān",
                "genealogy": "Kinda → Thawr → ʿUfayr → ʿAdī → al-Ḥārith → Kahlān",
                "region": "Ḥaḍramawt / Central Arabia",
                "history": "Pre-Islamic kingdom in Najd (5th-6th century CE); Imruʾ al-Qays the poet was Kindī",
                "main_settlement": "Ḥaḍramawt region",
                "economy": "Trade, agriculture, connections to Yemen",
                "ridda_role": "Major rebellion under al-Ashʿath ibn Qays",
                "ridda_leader": "al-Ashʿath ibn Qays al-Kindī",
                "key_battle": "Siege of al-Nujayr fortress (12 AH)",
                "siege_details": "Prolonged siege; al-Ashʿath attempted treacherous surrender",
                "outcome": "Defeated; al-Ashʿath captured trying to escape",
                "aftermath": "al-Ashʿath pardoned; married Abū Bakr's sister; led Muslim forces later",
                "notable_members": ["al-Ashʿath ibn Qays", "Imruʾ al-Qays (pre-Islamic poet-king)"],
                "sources": ["EI2: Kinda", "Shoufani 1972", "Donner 1981: 140-145"],
                "scholarly_note": "al-Ashʿath's rehabilitation shows Abū Bakr's reconciliation policy"
            },
            "azd": {
                "name_ei3": "al-Azd",
                "confederation": "Qaḥṭān (Kahlān branch)",
                "genealogy": "Azd → al-Ghawth → Nabt → Mālik → Zayd → Kahlān → Sabāʾ",
                "region": "Oman, Yemen, scattered throughout Arabia",
                "branches": [
                    {"name": "Azd ʿUmān", "location": "Oman", "ridda": "Rebelled under Laqīṭ"},
                    {"name": "Azd Shanūʾa", "location": "Sarāt mountains", "ridda": "Various"},
                    {"name": "Azd al-Sarāt", "location": "Yemen highlands", "ridda": "Various"}
                ],
                "history": "Major South Arabian tribe with widespread diaspora",
                "ridda_role": "Oman branch followed Laqīṭ ibn Mālik (kingship claim, not prophecy)",
                "ridda_leader": "Laqīṭ ibn Mālik Dhū l-Tāj",
                "leader_claim": "Restored pre-Islamic kingship (not prophetic claim)",
                "key_battle": "Battle of Dibā (11-12 AH)",
                "outcome": "Defeated; Laqīṭ killed at Dibā",
                "aftermath": "Submitted; Azd became important in Basra and Iraq",
                "notable_members": ["Laqīṭ ibn Mālik", "al-Muhallab ibn Abī Ṣufra (later)"],
                "sources": ["EI2: Azd", "Donner 1981"]
            },
            "madhij": {
                "name_ei3": "Madhḥij",
                "confederation": "Qaḥṭān (Kahlān branch)",
                "genealogy": "Madhḥij → Mālik → Udad → Zayd → Kahlān",
                "region": "Yemen (highlands and lowlands)",
                "population": "Major Yemeni confederation",
                "sub_tribes": [
                    {"name": "ʿAns", "ridda_role": "Followed al-Aswad al-ʿAnsī"},
                    {"name": "Murād", "ridda_role": "Participated in assassination of al-Aswad"},
                    {"name": "Saʿd al-ʿAshīra", "ridda_role": "Mixed loyalties"},
                    {"name": "al-Ḥārith", "ridda_role": "Various"}
                ],
                "ridda_role": "ʿAns branch followed al-Aswad; other branches divided",
                "ridda_leader": "al-Aswad al-ʿAnsī (from ʿAns)",
                "leader_claim": "Prophet (Dhū l-Khimār - 'of the Veil')",
                "key_event": "Assassination of al-Aswad in Ṣanʿāʾ before main campaigns",
                "assassins": "Qays ibn Makshūḥ, Fīrūz al-Daylamī, others",
                "outcome": "al-Aswad killed; tribes gradually returned to Islam",
                "aftermath": "Major participants in conquest of Iraq (Qādisiyya, etc.)",
                "notable_members": ["al-Aswad al-ʿAnsī", "Qays ibn Makshūḥ", "ʿAmr ibn Maʿdīkarib"],
                "sources": ["EI2: Madhḥidj"]
            },
            "himyar": {
                "name_ei3": "Ḥimyar",
                "confederation": "Qaḥṭān",
                "genealogy": "Ḥimyar → Sabāʾ → Yashjub → Yaʿrub → Qaḥṭān",
                "region": "Yemen (southern highlands)",
                "history": "Ancient South Arabian kingdom; ruled Yemen pre-Islam; Jewish period (Dhū Nuwās)",
                "capital": "Ẓafār",
                "economy": "Agriculture (terraces), frankincense trade",
                "pre_islamic": "Kingdom collapsed ~570s; Persian/Abyssinian interventions",
                "ridda_role": "Some factions supported al-Aswad; complex local politics",
                "outcome": "Mixed - submitted after al-Aswad's death",
                "sources": ["EI2: Ḥimyar"]
            },
            "bakr_ibn_wail": {
                "name_ei3": "Bakr ibn Wāʾil",
                "confederation": "ʿAdnān (Rabīʿa branch)",
                "genealogy": "Bakr → Wāʾil → Qāsiṭ → Hinb → Afṣā → Rabīʿa",
                "region": "Eastern Arabia / Bahrain / Lower Iraq",
                "territory": "From Bahrain coast to lower Euphrates",
                "sub_tribes": ["Shayban", "ʿIjl", "Yashkur", "Ḍubayʿa", "Qays ibn Thaʿlaba"],
                "pre_islamic": "Fought Persians at Dhū Qār (~610 CE) - Arab victory",
                "ridda_role": "Some elements rebelled in Bahrain under al-Ḥuṭam",
                "ridda_leader": "al-Ḥuṭam ibn Ḍubayʿa",
                "outcome": "Pacified after Bahrain campaign by al-ʿAlāʾ ibn al-Ḥaḍramī",
                "aftermath": "Important in conquest of Iraq; al-Muthannā ibn Ḥāritha from Shayban",
                "notable_members": ["al-Ḥuṭam ibn Ḍubayʿa", "al-Muthannā ibn Ḥāritha (later)"],
                "sources": ["EI2: Bakr b. Wāʾil"]
            },
            "abd_al_qays": {
                "name_ei3": "ʿAbd al-Qays",
                "confederation": "ʿAdnān (Rabīʿa branch)",
                "genealogy": "ʿAbd al-Qays → Afṣā → Rabīʿa",
                "region": "Bahrain / Eastern Arabia",
                "main_settlement": "Hajar, al-Qaṭīf",
                "religion": "Significant Christian population pre-Islam",
                "ridda_role": "REMAINED LOYAL - did not apostatize",
                "loyal_leader": "al-Jārūd ibn Bishr",
                "role_in_ridda": "Supported Muslim forces against Bahrain rebels",
                "outcome": "Commended for loyalty; strengthened position",
                "notable_members": ["al-Jārūd ibn Bishr", "al-Mundhir ibn Sāwā"],
                "sources": ["EI2: ʿAbd al-Ḳays"],
                "scholarly_note": "Important example of tribe that remained loyal throughout"
            },
            "qudaa": {
                "name_ei3": "Quḍāʿa",
                "confederation": "Disputed - claimed by both Qaḥṭān and ʿAdnān",
                "genealogy": "Genealogy disputed: Maʿadd (ʿAdnān) or Ḥimyar (Qaḥṭān)?",
                "region": "Northern Arabia / Syrian borders",
                "territory": "From northern Ḥijāz to Syrian frontier",
                "sub_tribes": ["Kalb", "Bahrāʾ", "Tanūkh", "Juhayna", "Balī", "ʿUdhra"],
                "ridda_role": "Some elements rebelled; generally minor compared to Najd/Yemen",
                "outcome": "Pacified; became important in Syrian futūḥāt",
                "aftermath": "Kalb especially important in Umayyad Syria",
                "sources": ["EI2: Ḳuḍāʿa"]
            },
            "tayy": {
                "name_ei3": "Ṭayyiʾ",
                "confederation": "Qaḥṭān (Kahlān branch)",
                "genealogy": "Ṭayyiʾ → Adad → Kahlān",
                "region": "Najd (northern) / Jabal Shammar",
                "main_settlement": "Aja and Salma mountains (Jabal Ṭayyiʾ)",
                "ridda_role": "Initially wavered; some followed Ṭulayḥa briefly",
                "loyal_leader": "ʿAdī ibn Ḥātim al-Ṭāʾī",
                "outcome": "Submitted relatively quickly; ʿAdī's influence crucial",
                "notable_members": ["ʿAdī ibn Ḥātim", "Ḥātim al-Ṭāʾī (pre-Islamic, proverbial generosity)"],
                "sources": ["EI2: Ṭayyiʾ"],
                "note": "ʿAdī ibn Ḥātim remained loyal and helped pacify tribe"
            },
            "mahra": {
                "name_ei3": "Mahra",
                "confederation": "Qaḥṭān (South Arabian)",
                "region": "Mahra (between Ḥaḍramawt and Oman)",
                "territory": "Remote southeastern coast",
                "language": "Spoke South Arabian language (Mahri) - not Arabic",
                "economy": "Fishing, pastoralism, frankincense",
                "ridda_role": "Rebelled after Oman campaign",
                "commanders": "ʿIkrima ibn Abī Jahl / ʿArfaja ibn Harthama",
                "outcome": "Subjugated (12 AH)",
                "aftermath": "Remote region; pacified late in campaigns",
                "sources": ["EI2: Mahra"]
            },
            "rabiya": {
                "name_ei3": "Rabīʿa ibn ʿĀmir",
                "confederation": "ʿAdnān",
                "region": "al-Baḥrayn / Yamāma borders",
                "ridda_role": "Some sections rebelled in Bahrain area",
                "outcome": "Submitted after Bahrain campaign",
                "sources": ["Balādhurī: Futūḥ"]
            },
            "banu_najiya": {
                "name_ei3": "Banū Nājiya",
                "confederation": "ʿAdnān (Ḥanīfa-related)",
                "region": "al-Baḥrayn",
                "ridda_role": "Participated in Bahrain rebellion",
                "outcome": "Defeated; some captured",
                "sources": ["al-Ṭabarī"]
            },
            "namir_wa_taghlib": {
                "name_ei3": "al-Namir wa-Taghlīb",
                "confederation": "ʿAdnān (Rabīʿa branch)",
                "genealogy": "Taghlīb and al-Namir both from Wāʾil → Rabīʿa",
                "region": "Upper Mesopotamia / Jazīra",
                "ridda_role": "Elements involved in Iraq/Jazīra disturbances",
                "christian_element": "Significant Christian population among Taghlīb",
                "outcome": "Submitted; important in Jazīra campaigns",
                "sources": ["EI2: Taghlīb"]
            },
            "banu_kinana": {
                "name_ei3": "Banū Kināna",
                "confederation": "ʿAdnān (Muḍar branch)",
                "genealogy": "Kināna → Khuzayma → Mudrika → Ilyās → Muḍar",
                "region": "Ḥijāz / Tihāma",
                "territory": "Between Mecca and coastal Tihāma",
                "ridda_status": "LOYAL",
                "ridda_role": "Some elements wavered; generally loyal",
                "note": "Quraysh is a sub-group of Kināna",
                "outcome": "Remained largely loyal",
                "sources": ["EI2: Kināna"]
            },
            # ================================================================
            # TRIBES THAT DID NOT PARTICIPATE IN RIDDA (LOYAL)
            # ================================================================
            "quraysh": {
                "name_ei3": "Quraysh",
                "confederation": "ʿAdnān (Muḍar → Kināna branch)",
                "genealogy": "Quraysh → Fihr → Mālik → al-Naḍr → Kināna → Khuzayma → Mudrika",
                "region": "Ḥijāz (Mecca)",
                "main_settlement": "Mecca",
                "economy": "Trade (caravans), custodians of Kaʿba",
                "population": "Medium - but politically dominant",
                "pre_islamic": "Guardians of Kaʿba; major traders; tribe of Prophet Muḥammad",
                "ridda_status": "LOYAL",
                "ridda_role": "Did NOT participate in Ridda - core of early Muslim leadership",
                "reason_loyal": "Tribe of Prophet; early converts held power; Abū Bakr and ʿUmar were Quraysh",
                "key_figures": ["Abū Bakr al-Ṣiddīq (caliph)", "ʿUmar ibn al-Khaṭṭāb", "Khālid ibn al-Walīd"],
                "sub_clans": ["Banū Hāshim", "Banū Umayya", "Banū Makhzūm", "Banū Taym", "Banū ʿAdī", "Banū Zuhra"],
                "aftermath": "Provided leadership for futūḥāt and caliphate",
                "sources": ["EI2: Ḳuraysh", "Donner 1981"]
            },
            "ansar_aws": {
                "name_ei3": "al-Aws",
                "confederation": "Qaḥṭān (Azd branch via Ghassān)",
                "genealogy": "Aws → Ḥāritha → Thaʿlaba → ʿAmr → ʿĀmir → Ghassān → Azd",
                "region": "Medina (Yathrib)",
                "main_settlement": "Medina - southern districts",
                "pre_islamic": "Rival of Khazraj; converted 622 CE",
                "ridda_status": "LOYAL",
                "ridda_role": "Did NOT participate - core Anṣār, defended Medina",
                "reason_loyal": "Early converts (622 CE); hosted Prophet; integral to Muslim community",
                "key_figures": ["Saʿd ibn Muʿādh (d. 627)", "Usayd ibn Ḥuḍayr"],
                "note": "Together with Khazraj formed the Anṣār (Helpers)",
                "aftermath": "Core of Medinan garrison; some participated in futūḥāt",
                "sources": ["EI2: al-Aws"]
            },
            "ansar_khazraj": {
                "name_ei3": "al-Khazraj",
                "confederation": "Qaḥṭān (Azd branch via Ghassān)",
                "genealogy": "Khazraj → Ḥāritha → Thaʿlaba → ʿAmr → ʿĀmir → Ghassān → Azd",
                "region": "Medina (Yathrib)",
                "main_settlement": "Medina - northern districts",
                "pre_islamic": "Rival of Aws; first to invite Prophet to Medina",
                "ridda_status": "LOYAL",
                "ridda_role": "Did NOT participate - core Anṣār, defended Medina",
                "reason_loyal": "First to pledge allegiance at ʿAqaba; hosted Prophet",
                "key_figures": ["Saʿd ibn ʿUbāda", "ʿAbdallāh ibn Ubayy (munāfiq leader, d. 631)"],
                "note": "Larger of the two Anṣār tribes",
                "aftermath": "Defended Medina; participated in futūḥāt",
                "sources": ["EI2: Khazradj"]
            },
            "thaqif": {
                "name_ei3": "Thaqīf",
                "confederation": "ʿAdnān (Qays ʿAylān branch)",
                "genealogy": "Thaqīf → Qasī → Munabbih → Bakr → Hawāzin",
                "region": "Ḥijāz (al-Ṭāʾif)",
                "main_settlement": "al-Ṭāʾif",
                "economy": "Agriculture (grapes, fruits), trade",
                "pre_islamic": "Last major tribe to convert (630 CE); initially resisted",
                "ridda_status": "LOYAL",
                "ridda_role": "Did NOT participate in Ridda despite late conversion",
                "reason_loyal": "Strong leadership; integrated into Muslim state; economic ties to Mecca",
                "key_figures": ["al-Mughīra ibn Shuʿba", "Abū Sufyān ibn Ḥarb (married into)"],
                "notable_event": "Siege of al-Ṭāʾif (630 CE) - converted shortly after",
                "aftermath": "Major participants in Iraq conquest; al-Mughīra became governor",
                "sources": ["EI2: Thaḳīf"]
            },
            "hawazin": {
                "name_ei3": "Hawāzin",
                "confederation": "ʿAdnān (Qays ʿAylān branch)",
                "genealogy": "Hawāzin → Manṣūr → ʿIkrima → Qays ʿAylān → Muḍar",
                "region": "Najd / Ḥijāz borders",
                "territory": "East and southeast of Mecca",
                "pre_islamic": "Fought Muslims at Ḥunayn (630 CE); defeated and converted",
                "ridda_status": "MOSTLY_LOYAL",
                "ridda_role": "Largely did NOT participate after Ḥunayn defeat",
                "reason_loyal": "Thoroughly defeated at Ḥunayn; ransomed captives created ties",
                "sub_tribes": ["Thaqīf (see separate)", "Saʿd ibn Bakr (Prophet's wet-nurse tribe)", "Naṣr", "Jusham"],
                "key_figures": ["Mālik ibn ʿAwf (converted after Ḥunayn)"],
                "aftermath": "Participated in futūḥāt",
                "sources": ["EI2: Hawāzin"]
            },
            "hudhayl": {
                "name_ei3": "Hudhayl",
                "confederation": "ʿAdnān (Muḍar branch)",
                "genealogy": "Hudhayl → Mudrika → Ilyās → Muḍar",
                "region": "Ḥijāz (between Mecca and al-Ṭāʾif)",
                "territory": "Mountains east of Mecca",
                "pre_islamic": "Known for poetry; some early converts",
                "ridda_status": "LOYAL",
                "ridda_role": "Did NOT participate in Ridda",
                "reason_loyal": "Close to Mecca; early integration; no charismatic rebel leader",
                "note": "Known for archery and poetry",
                "sources": ["EI2: Hudhayl"]
            },
            "daws": {
                "name_ei3": "Daws",
                "confederation": "Qaḥṭān (Azd branch)",
                "genealogy": "Daws → Ḥārith → Zahrān → Kaʿb → ʿAbdallāh → Mālik → Naṣr → Azd",
                "region": "Sarāt mountains (between Ḥijāz and Yemen)",
                "pre_islamic": "Abū Hurayra's tribe; converted early",
                "ridda_status": "LOYAL",
                "ridda_role": "Did NOT participate in Ridda",
                "reason_loyal": "Early conversion; Abū Hurayra's influence; integrated into community",
                "key_figures": ["Abū Hurayra (major ḥadīth transmitter)", "al-Ṭufayl ibn ʿAmr al-Dawsī"],
                "aftermath": "Abū Hurayra became one of most prolific ḥadīth narrators",
                "sources": ["EI2: Daws"]
            },
            "muzayna": {
                "name_ei3": "Muzayna",
                "confederation": "ʿAdnān (Muḍar branch)",
                "genealogy": "Muzayna → ʿAmr → Udd → Ṭābikha → Ilyās → Muḍar",
                "region": "Ḥijāz (near Medina)",
                "territory": "Between Medina and Red Sea coast",
                "pre_islamic": "Early converts; participated in battles with Prophet",
                "ridda_status": "LOYAL",
                "ridda_role": "Did NOT participate in Ridda",
                "reason_loyal": "Close to Medina; early conversion; strong ties to Prophet",
                "key_figures": ["Nuʿaym ibn Masʿūd", "Bilāl ibn al-Ḥārith"],
                "sources": ["EI2: Muzayna"]
            },
            "juhayna": {
                "name_ei3": "Juhayna",
                "confederation": "Disputed (Quḍāʿa)",
                "genealogy": "Juhayna → Zayd → Līth → Suwd → Aslum → al-Ḥāf → Quḍāʿa",
                "region": "Ḥijāz (coastal area north of Yanbuʿ)",
                "territory": "Red Sea coast, north of Medina",
                "pre_islamic": "Early delegation to Prophet; guides for Muslim campaigns",
                "ridda_status": "LOYAL",
                "ridda_role": "Did NOT participate in Ridda",
                "reason_loyal": "Close relationship with Prophet; provided guides and support",
                "key_figures": ["ʿAmr ibn Murra al-Juhanī"],
                "aftermath": "Participated in conquest of Egypt",
                "sources": ["EI2: Djuhayna"]
            },
            "aslam": {
                "name_ei3": "Aslam",
                "confederation": "Disputed (Quḍāʿa / Khuzāʿa)",
                "region": "Ḥijāz (near Medina)",
                "pre_islamic": "Allied with Khuzāʿa; early converts",
                "ridda_status": "LOYAL",
                "ridda_role": "Did NOT participate in Ridda",
                "reason_loyal": "Very early conversion; close to Medina",
                "key_figures": ["Salama ibn al-Akwaʿ", "Hind ibn Ḥāritha"],
                "sources": ["EI2: Aslam"]
            },
            "ghifar": {
                "name_ei3": "Ghifār",
                "confederation": "ʿAdnān (Kināna branch)",
                "genealogy": "Ghifār → Mulayḥ → Ḍamra → Bakr → ʿAbd Manāt → Kināna",
                "region": "Ḥijāz (between Mecca and Medina)",
                "territory": "Along the Mecca-Medina road",
                "pre_islamic": "Converted early; Abū Dharr al-Ghifārī was early convert",
                "ridda_status": "LOYAL",
                "ridda_role": "Did NOT participate in Ridda",
                "reason_loyal": "Very early conversion (Abū Dharr); close to Muslim centers",
                "key_figures": ["Abū Dharr al-Ghifārī (major companion)"],
                "note": "Abū Dharr was one of first converts outside Mecca",
                "sources": ["EI2: Ghifār"]
            },
            "khuzaa": {
                "name_ei3": "Khuzāʿa",
                "confederation": "Disputed (Qaḥṭān Azd or ʿAdnān)",
                "genealogy": "Disputed - either Azd or Muḍar origin",
                "region": "Ḥijāz (around Mecca)",
                "territory": "Marr al-Ẓahrān and coastal areas",
                "pre_islamic": "Allied with Prophet against Quraysh; custodians of Kaʿba before Quraysh",
                "ridda_status": "LOYAL",
                "ridda_role": "Did NOT participate in Ridda",
                "reason_loyal": "Allied with Prophet since Ḥudaybiyya; helped trigger conquest of Mecca",
                "key_figures": ["ʿAmr ibn Sālim al-Khuzāʿī", "Budayl ibn Warqāʾ"],
                "historical_note": "Their attack by Banū Bakr triggered conquest of Mecca",
                "sources": ["EI2: Khuzāʿa"]
            },
            "hamdan": {
                "name_ei3": "Hamdān",
                "confederation": "Qaḥṭān (Kahlān branch)",
                "genealogy": "Hamdān → Aws.lān → Mālik → Zayd → Kahlān",
                "region": "Yemen (northern highlands)",
                "main_settlement": "Around Ṣanʿāʾ and Ṣaʿda",
                "pre_islamic": "Major Yemeni tribe; converted 10 AH",
                "ridda_status": "MOSTLY_LOYAL",
                "ridda_role": "Largely did NOT join Ridda; helped suppress al-Aswad",
                "reason_loyal": "Rivaled Madhḥij; al-Aswad was their enemy",
                "key_figures": ["ʿAlī ibn Abī Ṭālib wrote to them"],
                "sub_tribes": ["Ḥāshid", "Bakīl"],
                "aftermath": "Major participants in Iraq conquests",
                "sources": ["EI2: Hamdān"]
            },
            "bajila": {
                "name_ei3": "Bajīla",
                "confederation": "Qaḥṭān (disputed, possibly Azd)",
                "region": "Sarāt mountains / Yemen borders",
                "pre_islamic": "Mountain tribe; converted late but firmly",
                "ridda_status": "LOYAL",
                "ridda_role": "Did NOT participate in Ridda",
                "reason_loyal": "Integrated through conversion; no rebel leadership emerged",
                "key_figures": ["Jarīr ibn ʿAbdallāh al-Bajālī"],
                "aftermath": "Jarīr became important commander in Iraq",
                "sources": ["EI2: Badjīla"]
            },
            "lakhm": {
                "name_ei3": "Lakhm",
                "confederation": "Qaḥṭān",
                "genealogy": "Lakhm → ʿAdī → al-Ḥārith → Murr → Adad",
                "region": "Southern Iraq / Ḥīra",
                "main_settlement": "al-Ḥīra (Lakhmid capital)",
                "pre_islamic": "Lakhmid kingdom (Arab client of Sasanians); Christian elements",
                "ridda_status": "NOT_APPLICABLE",
                "ridda_role": "Outside Arabia proper; later incorporated during futūḥāt",
                "note": "Lakhmid kingdom had ended 602 CE; tribes absorbed into Muslim state during Iraq conquest",
                "sources": ["EI2: Lakhm", "EI2: al-Ḥīra"]
            },
            "ghassan": {
                "name_ei3": "Ghassān",
                "confederation": "Qaḥṭān (Azd branch)",
                "genealogy": "Ghassān → ʿĀmir → Māzin → al-Azd",
                "region": "Southern Syria / Transjordan",
                "main_settlement": "Jābiya (Ghassanid capital)",
                "pre_islamic": "Ghassanid kingdom (Arab client of Byzantines); Christian",
                "ridda_status": "NOT_APPLICABLE",
                "ridda_role": "Outside Arabia proper; later incorporated during futūḥāt",
                "note": "Christian Arab kingdom; gradually absorbed during Syrian conquest",
                "key_figures": ["Jabala ibn al-Ayham (last Ghassanid king)"],
                "sources": ["EI2: Ghassān"]
            }
        },
        "tribal_analysis": {
            "patterns": [
                "Both ʿAdnānī (Northern) and Qaḥṭānī (Southern) tribes participated in Ridda",
                "Charismatic leadership (false prophets) was crucial factor in mobilizing rebellion",
                "Some tribes divided internally along sub-tribal lines (e.g., Tamīm)",
                "Tribes on Medina's periphery more likely to rebel than core Ḥijāzī tribes",
                "Economic motivations (zakāt refusal) combined with religious/political claims",
                "Remote regions (Mahra, Oman) required separate campaigns",
                "Tribes closest to Mecca/Medina remained loyal (Quraysh, Anṣār, Thaqīf)",
                "Early conversion and strong ties to Prophet correlated with loyalty",
                "Tribes defeated by Prophet shortly before his death (Hawāzin) did not rebel"
            ],
            "loyal_tribes": {
                "core_loyal": [
                    "Quraysh (Mecca) - tribe of Prophet and caliphs",
                    "al-Aws (Medina) - Anṣār",
                    "al-Khazraj (Medina) - Anṣār",
                    "Thaqīf (al-Ṭāʾif) - late but firm converts",
                    "Khuzāʿa - allied since Ḥudaybiyya"
                ],
                "early_converts_loyal": [
                    "Daws - Abū Hurayra's tribe",
                    "Ghifār - Abū Dharr's tribe",
                    "Muzayna - near Medina",
                    "Juhayna - coastal guides",
                    "Aslam - near Medina"
                ],
                "strategic_loyal": [
                    "ʿAbd al-Qays (Bahrain) - actively supported Muslims",
                    "Hamdān (Yemen) - rivals of Madhḥij",
                    "Bajīla - mountain tribe, firm converts"
                ],
                "reasons_for_loyalty": [
                    "Proximity to Mecca/Medina",
                    "Early conversion and integration",
                    "Personal ties to Prophet",
                    "Absence of charismatic rebel leaders",
                    "Recent military defeat (e.g., Hawāzin at Ḥunayn)",
                    "Rivalry with rebel tribes"
                ]
            },
            "rebel_tribes": {
                "major_rebels": [
                    "Banū Ḥanīfa - Musaylima (largest threat)",
                    "Banū Asad - Ṭulayḥa",
                    "Madhḥij (ʿAns) - al-Aswad",
                    "Kinda - al-Ashʿath",
                    "Azd (Oman) - Laqīṭ"
                ],
                "allied_rebels": [
                    "Ghaṭafān - allied with Ṭulayḥa",
                    "Bakr ibn Wāʾil (elements) - Bahrain"
                ],
                "divided_tribes": [
                    "Banū Tamīm - Mālik/Sajāḥ vs. loyal elements",
                    "Banū ʿĀmir - some rebelled, some loyal",
                    "Ḥimyar - factions supported al-Aswad"
                ],
                "reasons_for_rebellion": [
                    "Distance from Medina (periphery)",
                    "Charismatic alternative leaders (false prophets)",
                    "Economic grievances (zakāt)",
                    "Weak ties to Prophet",
                    "Pre-Islamic autonomy and pride",
                    "Late or forced conversion"
                ]
            },
            "outside_arabia": {
                "not_applicable": [
                    "Lakhm (Iraq) - Lakhmid kingdom ended; later absorbed",
                    "Ghassān (Syria) - Christian clients of Byzantium; later absorbed"
                ],
                "note": "These tribes were outside the Arabian Peninsula proper and were incorporated during the futūḥāt rather than Ridda Wars"
            },
            "incorporation_modes": {
                "SUBJUGATION": "Military defeat followed by forced submission (most common for rebels)",
                "SUBMISSION": "Voluntary return without battle (e.g., Banū Sulaym)",
                "MIXED": "Internal tribal divisions led to varied outcomes (e.g., Banū Tamīm)",
                "LOYAL": "Did not rebel; provided support to Muslim campaigns"
            },
            "aftermath_patterns": [
                "Many former rebels became leaders in futūḥāt (al-Ashʿath, Ṭulayḥa)",
                "Abū Bakr's policy combined firmness with reconciliation",
                "Tribal structures remained intact; redirected toward external conquest",
                "Loyal tribes rewarded with leadership positions",
                "All tribes eventually participated in conquests regardless of Ridda role"
            ],
            "statistics": {
                "total_major_tribes": 35,
                "rebelled_fully": 8,
                "rebelled_partially": 5,
                "remained_loyal": 15,
                "outside_arabia": 2,
                "minor_or_unclear": 5
            }
        }
    },
    
    "battles": {
        "buzakha": {
            "name_ar": "معركة بزاخة",
            "name_ei3": "Battle of Buzākha",
            "date": "11 AH (c. August 632 CE)",
            "location": {"lat": 27.50, "lon": 43.50, "region": "Najd"},
            "muslim_commander": "Khālid ibn al-Walīd",
            "opponents": ["Ṭulayḥa ibn Khuwaylid", "Banū Asad", "Ghaṭafān"],
            "forces": {
                "muslim": "Unknown, several thousand",
                "rebel": "Asad + Ghaṭafān confederation, several thousand"
            },
            "description": "Decisive battle against Ṭulayḥa's coalition in central Arabia",
            "outcome": "Muslim victory; Ṭulayḥa fled to Syria",
            "significance": "Broke power of false prophet movement in Najd",
            "sources": ["Donner 1981: 101-110", "al-Ṭabarī I: 1870-1880"]
        },
        "aqraba": {
            "name_ar": "معركة عقرباء",
            "name_ei3": "Battle of ʿAqrabāʾ",
            "alternative_names": ["Battle of Yamāma", "Battle of the Garden of Death"],
            "date": "12 AH (c. December 632 - January 633 CE)",
            "location": {"lat": 24.20, "lon": 47.35, "region": "al-Yamāma"},
            "muslim_commander": "Khālid ibn al-Walīd",
            "opponents": ["Musaylima ibn Ḥabīb", "Banū Ḥanīfa"],
            "forces": {
                "muslim": "~13,000",
                "rebel": "~40,000 (estimates vary)"
            },
            "phases": [
                "Initial Muslim setback",
                "Khālid reorganizes army by tribal units",
                "Final assault",
                "Garden of Death - Ḥanīfa last stand in walled garden"
            ],
            "outcome": "Muslim victory; Musaylima killed by Waḥshī ibn Ḥarb",
            "casualties": {
                "muslim": "~1,200 killed including 700 ḥuffāẓ (Quran memorizers)",
                "rebel": "~7,000-21,000 killed (sources vary)"
            },
            "significance": [
                "Largest battle of Ridda Wars",
                "Ended most serious threat to Islamic state",
                "Heavy ḥuffāẓ casualties prompted Quran compilation"
            ],
            "sources": ["Kister 1986 (detailed study)", "Donner 1981: 115-128", "al-Ṭabarī I: 1930-1960"]
        },
        "diba": {
            "name_ar": "معركة دبا",
            "name_ei3": "Battle of Dibā",
            "date": "11-12 AH",
            "location": {"lat": 25.62, "lon": 56.27, "region": "Oman"},
            "muslim_commander": "Ḥudhayfa ibn Miḥṣan, ʿIkrima ibn Abī Jahl",
            "opponents": ["Laqīṭ ibn Mālik Dhū l-Tāj", "Azd of Oman"],
            "outcome": "Muslim victory; Laqīṭ killed",
            "significance": "Pacified Oman",
            "sources": ["Donner 1981: 135-140"]
        },
        "nujayr": {
            "name_ar": "حصار النجير",
            "name_ei3": "Siege of al-Nujayr",
            "date": "12 AH",
            "location": {"lat": 15.80, "lon": 48.50, "region": "Ḥaḍramawt"},
            "muslim_commander": "Ziyād ibn Labīd, al-Muhājir ibn Abī Umayya",
            "opponents": ["al-Ashʿath ibn Qays", "Kinda"],
            "type": "Siege",
            "outcome": "Kinda surrender; al-Ashʿath captured",
            "significance": "Final major engagement; ended organized Ridda resistance",
            "sources": ["Donner 1981: 140-145"]
        }
    },
    
    "terminology": {
        "ridda": {
            "arabic": "ردة",
            "transliteration": "ridda",
            "literal": "turning back, apostasy",
            "usage": "Technical term for apostasy from Islam",
            "scholarly_debate": "Whether events were religious apostasy or political secession",
            "donner_view": "Primarily political - tribes sought to break tax obligations",
            "traditional_view": "Religious apostasy requiring military response"
        },
        "nabi_kadhib": {
            "arabic": "نبي كاذب",
            "transliteration": "nabī kādhib",
            "meaning": "False prophet",
            "application": "Musaylima, al-Aswad, Ṭulayḥa, Sajāḥ"
        },
        "sadaqa": {
            "arabic": "صدقة",
            "transliteration": "ṣadaqa",
            "meaning": "Alms tax, charitable contribution",
            "context": "Central issue - tribes refused to pay after Prophet's death",
            "abu_bakr_statement": "By God, if they withhold from me a she-kid (ʿanāq) they used to pay to the Messenger of God, I will fight them for it"
        },
        "ahl_al_ridda": {
            "arabic": "أهل الردة",
            "transliteration": "ahl al-ridda",
            "meaning": "People of apostasy",
            "usage": "Collective term for rebel tribes"
        },
        "murtadd": {
            "arabic": "مرتد",
            "transliteration": "murtadd",
            "meaning": "Apostate (singular)",
            "plural": "murtaddūn (مرتدون)"
        }
    },
    
    "chronology": {
        "8_june_632": {
            "date_ah": "12 Rabīʿ al-Awwal 11 AH",
            "event": "Death of Prophet Muḥammad",
            "significance": "Trigger for Ridda Wars"
        },
        "june_632": {
            "date_ah": "11 AH",
            "event": "Saqīfa meeting; Abū Bakr elected caliph"
        },
        "summer_632": {
            "date_ah": "11 AH",
            "event": "Tribal delegations demand ṣadaqa return; Abū Bakr refuses"
        },
        "july_632": {
            "date_ah": "11 AH",
            "event": "Defense of Medina at Dhū l-Qaṣṣa"
        },
        "aug_sep_632": {
            "date_ah": "11 AH",
            "event": "Khālid's Najd campaign; Battle of Buzākha"
        },
        "sep_oct_632": {
            "date_ah": "11 AH",
            "event": "Tamīm campaign; execution of Mālik ibn Nuwayra"
        },
        "dec_632_jan_633": {
            "date_ah": "11-12 AH",
            "event": "Battle of ʿAqrabāʾ; death of Musaylima"
        },
        "633": {
            "date_ah": "12 AH",
            "event": "Final campaigns in Bahrain, Oman, Ḥaḍramawt"
        },
        "late_633": {
            "date_ah": "12 AH",
            "event": "Arabia unified; Khālid moves to Iraq (beginning of Futūḥāt)"
        }
    },
    
    "maps_and_geography": {
        "regions": {
            "yamama": {
                "name_ar": "اليمامة",
                "name_ei3": "al-Yamāma",
                "modern": "Central Saudi Arabia (Riyadh region)",
                "coordinates": {"lat": 24.15, "lon": 47.30},
                "description": "Fertile region in eastern Najd; Banū Ḥanīfa homeland",
                "key_sites": ["Ḥajr (capital)", "ʿAqrabāʾ (battle site)"]
            },
            "najd": {
                "name_ar": "نجد",
                "name_ei3": "Najd",
                "modern": "Central Saudi Arabia",
                "coordinates": {"lat": 25.00, "lon": 45.00},
                "description": "Central Arabian plateau",
                "key_sites": ["Buzākha", "al-Biṭāḥ", "Sumayra"]
            },
            "bahrayn": {
                "name_ar": "البحرين",
                "name_ei3": "al-Baḥrayn",
                "modern": "Eastern Saudi Arabia + Bahrain + Qatar",
                "note": "Medieval Bahrain was much larger than modern state",
                "coordinates": {"lat": 26.00, "lon": 50.00},
                "key_sites": ["Hajar", "Jawāthā", "al-Qaṭīf"]
            },
            "uman": {
                "name_ar": "عمان",
                "name_ei3": "ʿUmān",
                "modern": "Oman + UAE",
                "coordinates": {"lat": 23.50, "lon": 58.00},
                "key_sites": ["Dibā", "Ṣuḥār"]
            },
            "yaman": {
                "name_ar": "اليمن",
                "name_ei3": "al-Yaman",
                "modern": "Yemen",
                "coordinates": {"lat": 15.50, "lon": 44.00},
                "key_sites": ["Ṣanʿāʾ", "Najrān", "Maʾrib"]
            },
            "hadramawt": {
                "name_ar": "حضرموت",
                "name_ei3": "Ḥaḍramawt",
                "modern": "Eastern Yemen",
                "coordinates": {"lat": 16.00, "lon": 49.00},
                "key_sites": ["al-Nujayr"]
            }
        }
    },
    
    "research_questions": {
        "open_questions": [
            "Extent of genuine religious apostasy vs. political secession",
            "Actual size of armies involved (sources exaggerate)",
            "Precise chronology of events (sources disagree)",
            "Nature of Musaylima's religious teachings",
            "Role of economic factors (trade routes, agriculture)",
            "Christian influence on Sajāḥ's movement",
            "Long-term impact on tribal structures"
        ],
        "methodological_issues": [
            "Sources written 150+ years after events",
            "Theological framing affects narrative",
            "Numbers likely exaggerated",
            "Tribal biases in transmission",
            "Lost earlier sources (e.g., al-Wāqidī's Kitāb al-Ridda)"
        ]
    }
}


# ============================================================
# ENRICHMENT FUNCTIONS
# ============================================================

def add_coordinates(event):
    """Add geographic coordinates with fuzzy matching."""
    # Fields to check in order of specificity
    fields_to_check = ['battle_site_arabic', 'region_arabic']
    
    for field in fields_to_check:
        value = event.get(field, '')
        if not value:
            continue
            
        # Direct match
        if value in COORDINATES:
            coords = COORDINATES[value]
            event['_lat'] = coords['lat']
            event['_lon'] = coords['lon']
            event['_region_key'] = coords['region']
            event['_region_en'] = REGION_NAMES.get(coords['region'], {}).get('en', coords.get('en', ''))
            return
        
        # Fuzzy match - check if any key is contained in value or vice versa
        for key, coords in COORDINATES.items():
            if key in value or value in key:
                event['_lat'] = coords['lat']
                event['_lon'] = coords['lon']
                event['_region_key'] = coords['region']
                event['_region_en'] = REGION_NAMES.get(coords['region'], {}).get('en', coords.get('en', ''))
                return


def normalize_name(arabic_name, field_type='general'):
    """Normalize Arabic name to EI3 transliteration."""
    if not arabic_name:
        return None
    
    # Direct lookup
    if arabic_name in NAME_NORMALIZATION:
        return NAME_NORMALIZATION[arabic_name]
    
    # Partial match
    for ar, en in NAME_NORMALIZATION.items():
        if ar in arabic_name or arabic_name in ar:
            return en
    
    return None


def add_normalized_names(event):
    """Add normalized EI3 transliterations."""
    # Normalize tribe
    tribe_ar = event.get('tribe_arabic', '')
    norm_tribe = normalize_name(tribe_ar)
    if norm_tribe:
        event['_tribe_normalized'] = norm_tribe
    
    # Normalize commander
    cmd_ar = event.get('commander_arabic', '')
    norm_cmd = normalize_name(cmd_ar)
    if norm_cmd:
        event['_commander_normalized'] = norm_cmd
    
    # Normalize rebel leader
    rebel_ar = event.get('rebel_leader_arabic', '')
    norm_rebel = normalize_name(rebel_ar)
    if norm_rebel:
        event['_rebel_normalized'] = norm_rebel


def find_cross_references(events):
    """Find matching events across sources."""
    # Group by tribe + region
    event_groups = defaultdict(list)
    
    for i, e in enumerate(events):
        key = (
            e.get('tribe_arabic', ''),
            e.get('region_arabic', ''),
            e.get('rebel_leader_arabic', '')
        )
        if any(key):
            event_groups[key].append(i)
    
    # Mark cross-references
    for key, indices in event_groups.items():
        if len(indices) > 1:
            # Check if from different sources
            sources = set(events[i].get('_source') for i in indices)
            if len(sources) > 1:
                for i in indices:
                    other_ids = [events[j].get('_event_id') for j in indices if j != i]
                    events[i]['_cross_refs'] = other_ids


def enrich_events(events):
    """Apply all enrichments to events."""
    for event in events:
        add_coordinates(event)
        add_normalized_names(event)
    
    find_cross_references(events)
    
    return events


def events_to_csv_rows(events):
    """Convert events to flat CSV rows."""
    rows = []
    for e in events:
        row = {
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
            'commander_normalized': e.get('_commander_normalized', ''),
            'rebel_leader_arabic': e.get('rebel_leader_arabic', ''),
            'rebel_leader_english': e.get('rebel_leader_english', ''),
            'rebel_normalized': e.get('_rebel_normalized', ''),
            'battle_site_arabic': e.get('battle_site_arabic', ''),
            'battle_site_english': e.get('battle_site_english', ''),
            'confidence': e.get('confidence', ''),
            'notes': e.get('notes', ''),
            'evidence': ' | '.join(e.get('evidence', [])),
            'cross_refs': ', '.join(e.get('_cross_refs', [])) if e.get('_cross_refs') else '',
        }
        rows.append(row)
    return rows


def scholarly_to_csv_rows(supplement):
    """Convert scholarly supplement to multiple CSV files data."""
    csvs = {}
    
    # Bibliography
    bib_rows = []
    for cat, sources in supplement['bibliography'].items():
        if isinstance(sources, dict):
            for key, data in sources.items():
                if isinstance(data, dict):
                    row = {'category': cat, 'key': key}
                    row.update({k: str(v) if not isinstance(v, (list, dict)) else json.dumps(v, ensure_ascii=False) 
                               for k, v in data.items()})
                    bib_rows.append(row)
    csvs['bibliography'] = bib_rows
    
    # Campaign phases
    phase_rows = []
    for phase_id, data in supplement['campaign_phases']['phases'].items():
        row = {'phase_id': phase_id}
        for k, v in data.items():
            if isinstance(v, list):
                row[k] = ' | '.join(str(x) for x in v)
            elif isinstance(v, dict):
                row[k] = json.dumps(v, ensure_ascii=False)
            else:
                row[k] = v
        phase_rows.append(row)
    csvs['campaign_phases'] = phase_rows
    
    # False prophets
    prophet_rows = []
    for key, data in supplement['biographical_profiles']['false_prophets'].items():
        row = {'key': key}
        for k, v in data.items():
            if isinstance(v, list):
                row[k] = ' | '.join(str(x) for x in v)
            else:
                row[k] = v
        prophet_rows.append(row)
    csvs['false_prophets'] = prophet_rows
    
    # Commanders
    cmd_rows = []
    for key, data in supplement['biographical_profiles']['commanders'].items():
        row = {'key': key}
        for k, v in data.items():
            if isinstance(v, list):
                row[k] = ' | '.join(str(x) for x in v)
            else:
                row[k] = v
        cmd_rows.append(row)
    csvs['commanders'] = cmd_rows
    
    # Tribes
    tribe_rows = []
    for key, data in supplement['tribal_data']['tribes'].items():
        row = {'key': key}
        for k, v in data.items():
            if isinstance(v, list):
                row[k] = ' | '.join(str(x) for x in v)
            else:
                row[k] = v
        tribe_rows.append(row)
    csvs['tribes'] = tribe_rows
    
    # Battles
    battle_rows = []
    for key, data in supplement['battles'].items():
        row = {'key': key}
        for k, v in data.items():
            if isinstance(v, (list, dict)):
                row[k] = json.dumps(v, ensure_ascii=False)
            else:
                row[k] = v
        battle_rows.append(row)
    csvs['battles'] = battle_rows
    
    return csvs


def write_csv(filepath, rows):
    """Write rows to CSV file."""
    if not rows:
        return
    
    # Collect ALL fieldnames from ALL rows
    all_fields = set()
    for row in rows:
        all_fields.update(row.keys())
    fieldnames = sorted(list(all_fields))
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description='Enrich Ridda Wars dataset')
    parser.add_argument('--input', '-i', default='output/llm_results/ridda_combined.json')
    parser.add_argument('--output-dir', '-o', default='output/enriched')
    args = parser.parse_args()
    
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("RIDDA WARS DATA ENRICHMENT v2.1")
    print("=" * 60)
    
    # Load input
    if not input_path.exists():
        print(f"❌ Input not found: {input_path}")
        return
    
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    events = data.get('events', [])
    print(f"\n✓ Loaded {len(events)} events from {input_path.name}")
    
    # Enrich events
    print("\nEnriching events...")
    enriched_events = enrich_events(events)
    
    # Count enrichments
    geo_count = sum(1 for e in enriched_events if e.get('_lat'))
    norm_count = sum(1 for e in enriched_events if e.get('_tribe_normalized'))
    xref_count = sum(1 for e in enriched_events if e.get('_cross_refs'))
    
    print(f"  - Geocoded: {geo_count}/{len(events)}")
    print(f"  - Names normalized: {norm_count}/{len(events)}")
    print(f"  - Cross-references found: {xref_count}")
    
    # Show unmapped events
    unmapped = [e for e in enriched_events if not e.get('_lat')]
    if unmapped:
        print(f"\n  ⚠️ {len(unmapped)} events still unmapped:")
        for e in unmapped[:10]:
            print(f"     - {e.get('_event_id')}: {e.get('region_arabic', 'NO REGION')} / {e.get('region_english', '')}")
        if len(unmapped) > 10:
            print(f"     ... and {len(unmapped) - 10} more")
    
    # Update data
    data['events'] = enriched_events
    data['enrichment'] = {
        'version': '2.1',
        'date': datetime.now().isoformat(),
        'geocoded': geo_count,
        'normalized': norm_count,
        'cross_referenced': xref_count,
        'unmapped': len(unmapped)
    }
    
    # Save enriched JSON
    enriched_json = output_dir / 'ridda_combined_enriched.json'
    with open(enriched_json, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Saved: {enriched_json}")
    
    # Save enriched CSV
    enriched_csv = output_dir / 'ridda_combined_enriched.csv'
    csv_rows = events_to_csv_rows(enriched_events)
    write_csv(enriched_csv, csv_rows)
    print(f"✅ Saved: {enriched_csv}")
    
    # Save scholarly supplement JSON
    print("\nGenerating scholarly supplement...")
    scholarly_json = output_dir / 'ridda_combined_scholarly.json'
    with open(scholarly_json, 'w', encoding='utf-8') as f:
        json.dump(SCHOLARLY_SUPPLEMENT, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved: {scholarly_json}")
    
    # Save scholarly supplement CSVs
    scholarly_csvs = scholarly_to_csv_rows(SCHOLARLY_SUPPLEMENT)
    for name, rows in scholarly_csvs.items():
        csv_path = output_dir / f'ridda_combined_scholarly_{name}.csv'
        write_csv(csv_path, rows)
        print(f"✅ Saved: {csv_path}")
    
    print("\n" + "=" * 60)
    print("OUTPUT FILES:")
    print("=" * 60)
    print(f"  {output_dir}/")
    print(f"  ├── ridda_combined_enriched.json      (enriched primary data)")
    print(f"  ├── ridda_combined_enriched.csv       (enriched data as CSV)")
    print(f"  ├── ridda_combined_scholarly.json     (scholarly supplement)")
    print(f"  ├── ridda_combined_scholarly_bibliography.csv")
    print(f"  ├── ridda_combined_scholarly_campaign_phases.csv")
    print(f"  ├── ridda_combined_scholarly_false_prophets.csv")
    print(f"  ├── ridda_combined_scholarly_commanders.csv")
    print(f"  ├── ridda_combined_scholarly_tribes.csv")
    print(f"  └── ridda_combined_scholarly_battles.csv")
    print("=" * 60)


if __name__ == '__main__':
    main()