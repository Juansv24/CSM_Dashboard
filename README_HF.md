---
title: CSM Dashboard
emoji: 📊
colorFrom: blue
colorTo: green
sdk: docker
app_port: 7860
pinned: true
---

# CSM Dashboard - Hugging Face Spaces

Interactive analytics dashboard for exploring mentions of 75 CEV recommendations in Colombian territorial development plans across 1,028 municipalities.

## Features

- 📊 National overview with interactive choropleth map
- 🏘️ Municipal deep-dive with demographic filtering
- 🗺️ Departmental analysis with comparative statistics
- 🔍 Full-text search across 4.2M records
- 📈 Similarity threshold filtering (0.5-1.0)
- 🎛️ Advanced socioeconomic filters (PDET, IICA, MDM, IPM)

## Data

- **Total Records:** 4,215,337
- **Municipalities:** 1,028
- **Departments:** 33
- **Recommendations:** 75 (CEV)
- **Similarity Engine:** Semantic similarity matching

## Performance

- **First Load:** 30-60 seconds (one-time data loading)
- **Subsequent Loads:** <2 seconds
- **Query Latency:** <50ms
- **Concurrent Users:** 50+

## Technology

- **Framework:** Streamlit 1.28.1
- **Database:** DuckDB 1.1.3 (in-memory)
- **Data Format:** Apache Parquet (1.3GB)
- **Visualization:** Plotly
- **Data Processing:** Pandas 2.2.3

## Deployment

This Space uses Docker to run the Streamlit application. The Docker image will be built automatically when you create the Space.

### Dockerfile Details

- Base Image: `python:3.11-slim`
- Port: 7860 (standard for HF Spaces)
- Git LFS: Enabled for parquet file downloads
- Dependencies: All pinned versions in `requirements.txt`

## Local Testing

To test locally before deploying:

```bash
# Install dependencies
pip install -r requirements.txt

# Run Streamlit app
streamlit run app.py
```

The app will be available at `http://localhost:8501`

## Data Source

Data is stored in Git LFS within this repository:
```
Data/Data Final Dashboard.parquet
```

The parquet file is automatically downloaded during Space initialization.

## Troubleshooting

### Issue: "Parquet file not found"
- Ensure Git LFS has downloaded the file
- Check that `Data/` directory exists after cloning

### Issue: First load is slow
- Expected behavior (30-60 seconds for first data load)
- Subsequent loads will be instant

### Issue: Out of memory
- HF Spaces provides 16GB RAM (sufficient for this app)
- If issues persist, check DuckDB memory settings in `google_drive_client.py`

## Support

For issues or questions:
1. Check the logs in the Space settings
2. Review the deployment documentation
3. Open an issue on GitHub: https://github.com/Juansv24/CSM_Dashboard

## License

This project is part of the UNDP CSM (Comisión para el Esclarecimiento de la Verdad) initiative.
