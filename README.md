# HubSpot-BigQuery Pipeline

A Python-based pipeline for fetching, snapshotting, and processing data from HubSpot into Google BigQuery, enabling scalable historical analytics and dashboarding.

## Features

* Fetches and snapshots HubSpot data (companies, deals, owners, etc.)
* Processes snapshots and applies custom scoring/mapping logic
* Stores results in BigQuery for easy integration with Looker Studio (Google Data Studio) and other analytics tools
* Clean project structure with support for dev/prod configuration and secrets management
* Ready for containerization (Docker) and cloud deployment

## Project Structure

```
.
├── src/                # Main source code
│   ├── main.py         # Entry point
│   ├── fetch_hubspot_data.py
│   ├── recreate.py
│   └── ...             # Additional modules
├── config/             # Non-sensitive config files (e.g. settings, mappings)
│   └── settings.json
├── secrets/            # Secret credentials (EXCLUDED from git)
│   └── (your_service_account_key.json)
├── requirements.txt    # Python dependencies
├── .gitignore
├── README.md
```

> ⚠️ **Do not store or commit any secrets (API keys, service account files) in the repository.**
> All secret files should go in the `/secrets/` folder, which is git-ignored.

## Setup

1. **Clone the repository**

   ```bash
   git clone <your-repo-url>
   cd hubspot-bq-pipeline
   ```

2. **Set up a Python virtual environment**

   ```bash
   python3 -m venv myenv
   source myenv/bin/activate
   pip install -r requirements.txt
   ```

3. **Place credentials in `/secrets/`**

   * Download your Google Cloud service account key (JSON) and any HubSpot secrets.
   * Place them in the `/secrets/` folder.

4. **Configure settings**

   * Update `config/settings.json` and other config files as needed.

## Usage

* **Run the main pipeline:**

  ```bash
  python -m src.main
  ```

* **Reprocess snapshots (if scoring/mapping logic changes):**

  ```bash
  python -m src.recreate
  ```

* **Other modules:**

  * Use the appropriate entry point as needed.

## Deployment

* Project is ready for Docker, cloud function, or manual deployment.
* Keep dev/prod configs and secrets separated for safety.
* BigQuery dataset and table names should be configured via config files.

## Contributing

Feel free to open issues or PRs to improve code, automation, or analytics!

---

## License

MIT (or your preferred license)

---

## Author

Øystein Baeko (and contributors)
