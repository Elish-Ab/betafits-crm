# Legacy Scripts

This folder contains **archived, non-production scripts** from earlier versions of the 401k reporting system.  
These files are maintained for **reference and historical context only**.

They are **not executed** as part of the current report generation pipeline.

---

## 📄 401k_reports.py

This script was the original full end-to-end implementation that performed:

- Data extraction from Supabase  
- Data lookup from Airtable  
- Metric derivations  
- PDF filling and flattening  
- PDF upload and Airtable attachment

Over time, this logic was replaced by the modular, stable pipeline located in the project root.

### ⚠️ Important Notes

- This script is **no longer fully functional** due to changes in schema and API behavior.  
- It remains valuable as a **reference for historical logic**, including:  
  - Original data mappings  
  - Early transformation formulas  
  - Intended form-field relationships  

### ❌ Do NOT use this script in production.

---

## Purpose of This Folder

Keeping legacy scripts:

- Prevents accidental loss of original business logic  
- Helps with debugging and tracing historical choices  
- Keeps the main project folder clean and production-focused  

All active, supported code lives in the project root.
