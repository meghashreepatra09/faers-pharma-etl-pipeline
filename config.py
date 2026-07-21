FAERS_FILE_PATHS = {
    "demo": "DEMO25Q4.txt",
    "drug": "DRUG25Q4.txt",
    "indi": "INDI25Q4.txt",
    "outc": "OUTC25Q4.txt",
    "reac": "REAC25Q4.txt",
    "rpsr": "RPSR25Q4.txt",
    "ther": "THER25Q4.txt",
}

dedup_keys = {
    "demo": ["primaryid"],
    "drug": ["primaryid", "drug_seq"],
    "indi": ["primaryid", "indi_drug_seq", "indi_pt"],
    "outc": ["primaryid", "outc_cod"],
    "reac": ["primaryid", "pt"],
    "rpsr": ["primaryid", "rpsr_cod"],
    "ther": ["primaryid", "dsg_drug_seq", "start_dt", "end_dt"],
}

BRONZE_BASE_PATH = "/Volumes/workspace/bronze"
SILVER_BASE_PATH = "/Volumes/workspace/silver"
GOLD_BASE_PATH = "/Volumes/workspace/gold"