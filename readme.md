# GitHub Top Starred Repositories - Data Engineering Project

This project processes the top GitHub repositories using Apache Spark and stores the results in a PostgreSQL database.

# Tech Stack

- Apache Spark (PySpark)
- PostgreSQL
- Python 3.11
- JSON Data Files (Top GitHub repos per search term)

##  Extracted Tables

1. **`programming_lang`**  
   Columns: `language`, `repo_count`

2. **`organizations_stars`**  
   Columns: `org_name`, `total_stars`

3. **`search_terms_relevance`**  
   Columns: `search_term`, `relevance_score`  
   Formula: `1.5 * forks + 1.32 * subscribers + 1.04 * stars`

##  Running the Project

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
