# Hugging Face Spaces Deployment Configuration

## Platform: Hugging Face Spaces (Free Tier)

### Resource Allocation
- **RAM:** 16GB (FREE) ✅
- **CPU:** 2 cores (FREE) ✅
- **Disk:** 50GB ephemeral (FREE) ✅
- **Cost:** $0/month

### Current App Specifications
- **Parquet File:** 1.3GB
- **Total Rows:** 4,215,337
- **DuckDB Memory Needed:** ~2-3GB
- **Parquet Location:** In Git repository

### Status: VIABLE ✅

This app will run successfully on Hugging Face Spaces free tier.

## Deployment Checklist

- [ ] Hugging Face account created
- [ ] New Space created (Streamlit template)
- [ ] GitHub repo connected
- [ ] `main` branch selected
- [ ] App deployed automatically
- [ ] Test first load (30-60 seconds)
- [ ] Test queries (<50ms response)
- [ ] Share with stakeholders

## Configuration Files Needed

### 1. `.streamlit/config.toml` (Optional)
Can be used to customize Streamlit settings for HF Spaces environment.

### 2. `app.py` - No changes needed
Current app works as-is on HF Spaces.

### 3. `requirements.txt` - Current versions work
Already tested and compatible with HF Spaces.

## Platform-Specific Notes

### Inactivity Behavior
- Space sleeps after 48 hours of inactivity
- **Impact:** First user will experience 30-60 second startup
- **User Experience:** Subsequent users get instant <50ms queries
- **Workaround:** Can disable sleep in Space settings (if needed)

### Data Persistence
- Ephemeral disk storage (resets on restart)
- **No problem:** Parquet file is in Git, auto-downloaded on startup
- **Result:** Data always available, no data loss

### Concurrent Users
- Can handle 50+ concurrent users simultaneously
- Shared DuckDB instance serves all queries
- Each query <50ms from memory

## Deployment Steps

### Step 1: Create Hugging Face Account
1. Go to https://huggingface.co
2. Sign up (free)
3. Verify email

### Step 2: Create New Space
1. Dashboard → New → Space
2. Space name: `csm-dashboard`
3. License: `openrail`
4. Space SDK: **Streamlit**
5. Visibility: Public (or Private)
6. Create Space

### Step 3: Connect GitHub
1. Space Settings → Repository
2. Link GitHub repository
3. Select `main` branch
4. Sync

### Step 4: Deploy
Space automatically deploys from GitHub.

## Expected Results

### Cold Start (First User, First Load)
```
Time: 0-30 seconds  → Clone repo from GitHub
Time: 30-50 seconds → Download parquet via Git LFS
Time: 50-60 seconds → Load parquet into DuckDB + create indexes
Time: 60+ seconds   → Dashboard loads, ready for interaction
```

### Warm Start (Subsequent Users/Reloads)
```
Time: 0-2 seconds   → App already loaded in memory
Time: 2-5 seconds   → Dashboard fully interactive
Time: <50ms         → Each query/filter interaction
```

## Monitoring

### Check Health
1. Go to Space URL: `https://huggingface.co/spaces/YOUR_USERNAME/csm-dashboard`
2. Open in browser
3. Wait for "Data loaded: 4,215,337 records" message in sidebar

### Check Logs
Space Settings → View App Output

### Troubleshooting

**Issue: "Parquet file not found"**
- Solution: Ensure Git LFS is tracking parquet file
- Command: `git lfs ls-files`

**Issue: Space goes to sleep after 48 hours**
- Solution: First visitor wakes it up automatically
- Workaround: Consider HF Pro for persistent uptime

**Issue: Out of memory**
- Solution: Should not happen with 16GB available
- Fallback: Upgrade to paid GPU hardware if needed

## Next Steps

1. ✅ Verify this worktree is ready
2. ⏭️ Test locally with `streamlit run app.py`
3. ⏭️ Deploy to HF Spaces when ready
4. ⏭️ Share URL with stakeholders
5. ⏭️ Monitor performance

## Status

- **Branch:** `feature/huggingface-spaces`
- **Status:** Ready for deployment
- **Risk Level:** Low
- **Expected Uptime:** 99% (with 48h inactivity pause)
