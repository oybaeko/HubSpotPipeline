# src/hubspot_pipeline/hubspot_scoring/views/definitions.py

"""
SQL definitions for pipeline analytics views
"""

from typing import Dict, Any

# View 1: Current Pipeline Score by Owner
VIEW_CURRENT_PIPELINE_BY_OWNER = {
    "name": "vw_current_pipeline_by_owner",
    "description": "Current pipeline scoring by sales rep from latest snapshot",
    "sql": """
WITH latest_snapshot AS (
  SELECT MAX(snapshot_id) as snapshot_id, MAX(record_timestamp) as record_timestamp
  FROM `{project}.{dataset}.hs_pipeline_units_snapshot`
),
current_pipeline AS (
  SELECT 
    pus.snapshot_id,
    pus.record_timestamp,
    pus.owner_id,
    o.email as owner_email,
    CONCAT(COALESCE(o.first_name, ''), ' ', COALESCE(o.last_name, '')) as owner_name,
    pus.company_id,
    c.company_name,
    c.company_type,
    pus.deal_id,
    d.deal_name,
    d.deal_type,
    pus.combined_stage,
    pus.stage_level,
    pus.adjusted_score,
    pus.stage_source
  FROM `{project}.{dataset}.hs_pipeline_units_snapshot` pus
  JOIN latest_snapshot ls ON pus.snapshot_id = ls.snapshot_id
  LEFT JOIN `{project}.{dataset}.hs_owners` o ON pus.owner_id = o.owner_id
  LEFT JOIN `{project}.{dataset}.hs_companies` c ON pus.company_id = c.company_id
  LEFT JOIN `{project}.{dataset}.hs_deals` d ON pus.deal_id = d.deal_id
),
aggregated_pipeline AS (
  SELECT 
    snapshot_id,
    record_timestamp,
    owner_id,
    owner_email,
    owner_name,
    combined_stage,
    stage_level,
    adjusted_score,
    COUNT(*) as num_companies,
    SUM(adjusted_score) as total_stage_score,
    -- Company type breakdown
    STRING_AGG(DISTINCT company_type ORDER BY company_type) as company_types_in_stage,
    -- Deal type breakdown  
    STRING_AGG(DISTINCT deal_type ORDER BY deal_type) as deal_types_in_stage,
    -- Stage source breakdown
    STRING_AGG(DISTINCT stage_source ORDER BY stage_source) as stage_sources
  FROM current_pipeline
  GROUP BY 1,2,3,4,5,6,7,8
)
SELECT 
  ap.*,
  -- Calculate owner total across all stages
  SUM(total_stage_score) OVER (PARTITION BY owner_id) as owner_total_score,
  SUM(num_companies) OVER (PARTITION BY owner_id) as owner_total_companies
FROM aggregated_pipeline ap
ORDER BY owner_total_score DESC, owner_id, stage_level DESC
"""
}

# View 2: Pipeline Comparison (Current vs Previous Week)
VIEW_PIPELINE_COMPARISON = {
    "name": "vw_pipeline_comparison", 
    "description": "Pipeline score comparison between current and previous week snapshots",
    "sql": """
WITH latest_snapshot AS (
  SELECT MAX(snapshot_id) as snapshot_id, MAX(record_timestamp) as record_timestamp
  FROM `{project}.{dataset}.hs_pipeline_units_snapshot`
),
target_previous_date AS (
  SELECT 
    ls.snapshot_id as current_snapshot_id,
    ls.record_timestamp as current_record_timestamp,
    -- Target date: Sunday one week ago
    DATE_SUB(DATE(ls.record_timestamp), INTERVAL 7 DAY) as target_previous_date
  FROM latest_snapshot ls
),
previous_snapshot AS (
  SELECT 
    tpd.*,
    -- Find snapshot closest to Sunday one week ago
    (SELECT snapshot_id 
     FROM `{project}.{dataset}.hs_pipeline_units_snapshot` 
     WHERE DATE(record_timestamp) <= tpd.target_previous_date
     ORDER BY record_timestamp DESC 
     LIMIT 1) as previous_snapshot_id,
    (SELECT record_timestamp
     FROM `{project}.{dataset}.hs_pipeline_units_snapshot` 
     WHERE DATE(record_timestamp) <= tpd.target_previous_date
     ORDER BY record_timestamp DESC 
     LIMIT 1) as previous_record_timestamp
  FROM target_previous_date tpd
),
current_scores AS (
  SELECT 
    pus.owner_id,
    o.email as owner_email,
    CONCAT(COALESCE(o.first_name, ''), ' ', COALESCE(o.last_name, '')) as owner_name,
    SUM(pus.adjusted_score) as current_total_score,
    COUNT(*) as current_total_companies
  FROM `{project}.{dataset}.hs_pipeline_units_snapshot` pus
  JOIN previous_snapshot ps ON pus.snapshot_id = ps.current_snapshot_id
  LEFT JOIN `{project}.{dataset}.hs_owners` o ON pus.owner_id = o.owner_id
  GROUP BY 1,2,3
),
previous_scores AS (
  SELECT 
    pus.owner_id,
    SUM(pus.adjusted_score) as previous_total_score,
    COUNT(*) as previous_total_companies
  FROM `{project}.{dataset}.hs_pipeline_units_snapshot` pus
  JOIN previous_snapshot ps ON pus.snapshot_id = ps.previous_snapshot_id
  GROUP BY 1
)
SELECT 
  cs.owner_id,
  cs.owner_email,
  cs.owner_name,
  ps.current_snapshot_id,
  ps.current_record_timestamp,
  ps.previous_snapshot_id,
  ps.previous_record_timestamp,
  ps.target_previous_date,
  COALESCE(cs.current_total_score, 0) as current_total_score,
  COALESCE(cs.current_total_companies, 0) as current_total_companies,
  COALESCE(prev.previous_total_score, 0) as previous_total_score,
  COALESCE(prev.previous_total_companies, 0) as previous_total_companies,
  -- Calculate changes
  COALESCE(cs.current_total_score, 0) - COALESCE(prev.previous_total_score, 0) as score_change,
  COALESCE(cs.current_total_companies, 0) - COALESCE(prev.previous_total_companies, 0) as company_change,
  -- Calculate percentage change (handle division by zero)
  CASE 
    WHEN COALESCE(prev.previous_total_score, 0) = 0 THEN NULL
    ELSE ROUND((COALESCE(cs.current_total_score, 0) - COALESCE(prev.previous_total_score, 0)) / prev.previous_total_score * 100, 2)
  END as score_change_percent
FROM current_scores cs
CROSS JOIN previous_snapshot ps
LEFT JOIN previous_scores prev ON cs.owner_id = prev.owner_id
ORDER BY score_change DESC, cs.owner_id
"""
}

# View 3: Pipeline Changes (New/Deleted/Changed Companies)
VIEW_PIPELINE_CHANGES = {
    "name": "vw_pipeline_changes",
    "description": "Companies that changed status between current and previous week snapshots",
    "sql": """
WITH latest_snapshot AS (
  SELECT MAX(snapshot_id) as snapshot_id, MAX(record_timestamp) as record_timestamp
  FROM `{project}.{dataset}.hs_pipeline_units_snapshot`
),
target_previous_date AS (
  SELECT 
    ls.snapshot_id as current_snapshot_id,
    ls.record_timestamp as current_record_timestamp,
    DATE_SUB(DATE(ls.record_timestamp), INTERVAL 7 DAY) as target_previous_date
  FROM latest_snapshot ls
),
previous_snapshot AS (
  SELECT 
    tpd.*,
    (SELECT snapshot_id 
     FROM `{project}.{dataset}.hs_pipeline_units_snapshot` 
     WHERE DATE(record_timestamp) <= tpd.target_previous_date
     ORDER BY record_timestamp DESC 
     LIMIT 1) as previous_snapshot_id,
    (SELECT record_timestamp
     FROM `{project}.{dataset}.hs_pipeline_units_snapshot` 
     WHERE DATE(record_timestamp) <= tpd.target_previous_date
     ORDER BY record_timestamp DESC 
     LIMIT 1) as previous_record_timestamp
  FROM target_previous_date tpd
),
current_companies AS (
  SELECT 
    ps.current_snapshot_id,
    ps.current_record_timestamp,
    ps.previous_snapshot_id,
    ps.previous_record_timestamp,
    pus.company_id,
    pus.owner_id,
    o.email as owner_email,
    CONCAT(COALESCE(o.first_name, ''), ' ', COALESCE(o.last_name, '')) as owner_name,
    c.company_name,
    c.company_type,
    pus.deal_id,
    d.deal_name,
    d.deal_type,
    pus.lifecycle_stage as current_lifecycle_stage,
    pus.lead_status as current_lead_status,
    pus.deal_stage as current_deal_stage,
    pus.combined_stage as current_combined_stage,
    pus.stage_level as current_stage_level,
    pus.adjusted_score as current_adjusted_score
  FROM `{project}.{dataset}.hs_pipeline_units_snapshot` pus
  CROSS JOIN previous_snapshot ps
  LEFT JOIN `{project}.{dataset}.hs_owners` o ON pus.owner_id = o.owner_id
  LEFT JOIN `{project}.{dataset}.hs_companies` c ON pus.company_id = c.company_id
  LEFT JOIN `{project}.{dataset}.hs_deals` d ON pus.deal_id = d.deal_id
  WHERE pus.snapshot_id = ps.current_snapshot_id
),
previous_companies AS (
  SELECT 
    pus.company_id,
    pus.owner_id,
    c.company_type as previous_company_type,
    pus.deal_id as previous_deal_id,
    d.deal_type as previous_deal_type,
    pus.lifecycle_stage as previous_lifecycle_stage,
    pus.lead_status as previous_lead_status,
    pus.deal_stage as previous_deal_stage,
    pus.combined_stage as previous_combined_stage,
    pus.stage_level as previous_stage_level,
    pus.adjusted_score as previous_adjusted_score
  FROM `{project}.{dataset}.hs_pipeline_units_snapshot` pus
  CROSS JOIN previous_snapshot ps
  LEFT JOIN `{project}.{dataset}.hs_companies` c ON pus.company_id = c.company_id
  LEFT JOIN `{project}.{dataset}.hs_deals` d ON pus.deal_id = d.deal_id
  WHERE pus.snapshot_id = ps.previous_snapshot_id
),
company_changes AS (
  SELECT 
    cc.*,
    pc.previous_company_type,
    pc.previous_deal_id,
    pc.previous_deal_type,
    pc.previous_lifecycle_stage,
    pc.previous_lead_status,
    pc.previous_deal_stage,
    pc.previous_combined_stage,
    pc.previous_stage_level,
    pc.previous_adjusted_score,
    -- Determine change type
    CASE
      WHEN pc.company_id IS NULL THEN 'NEW'
      WHEN cc.company_id IS NULL THEN 'DELETED'
      WHEN (cc.current_lifecycle_stage != pc.previous_lifecycle_stage OR
            COALESCE(cc.current_lead_status, '') != COALESCE(pc.previous_lead_status, '') OR
            COALESCE(cc.current_deal_stage, '') != COALESCE(pc.previous_deal_stage, '')) THEN 'CHANGED'
      ELSE 'UNCHANGED'
    END as change_type,
    -- Calculate score impact
    COALESCE(cc.current_adjusted_score, 0) - COALESCE(pc.previous_adjusted_score, 0) as score_impact
  FROM current_companies cc
  FULL OUTER JOIN previous_companies pc ON cc.company_id = pc.company_id
)
-- Add deleted companies with proper score lookup from stage mapping
SELECT * FROM (
  SELECT 
    current_snapshot_id,
    current_record_timestamp,
    previous_snapshot_id,
    previous_record_timestamp,
    company_id,
    owner_id,
    owner_email,
    owner_name,
    company_name,
    company_type,
    deal_id,
    deal_name,
    deal_type,
    current_lifecycle_stage,
    current_lead_status,
    current_deal_stage,
    current_combined_stage,
    current_stage_level,
    current_adjusted_score,
    previous_company_type,
    previous_deal_id,
    previous_deal_type,
    previous_lifecycle_stage,
    previous_lead_status,
    previous_deal_stage,
    previous_combined_stage,
    previous_stage_level,
    previous_adjusted_score,
    change_type,
    score_impact
  FROM company_changes
  WHERE change_type IN ('NEW', 'CHANGED')
  
  UNION ALL
  
  -- Handle deleted companies (lookup disqualified score from stage mapping)
  SELECT 
    cc.current_snapshot_id,
    cc.current_record_timestamp,
    cc.previous_snapshot_id,
    cc.previous_record_timestamp,
    cc.company_id,
    cc.owner_id,
    cc.owner_email,
    cc.owner_name,
    cc.company_name,
    cc.previous_company_type as company_type,  -- Keep previous company type
    cc.previous_deal_id as deal_id,            -- Keep previous deal info
    NULL as deal_name,                         -- Deal is gone
    cc.previous_deal_type as deal_type,        -- Keep previous deal type
    'disqualified' as current_lifecycle_stage,
    NULL as current_lead_status,
    NULL as current_deal_stage,
    sm.combined_stage as current_combined_stage,
    sm.stage_level as current_stage_level,
    sm.adjusted_score as current_adjusted_score,
    cc.previous_company_type,
    cc.previous_deal_id,
    cc.previous_deal_type,
    cc.previous_lifecycle_stage,
    cc.previous_lead_status,
    cc.previous_deal_stage,
    cc.previous_combined_stage,
    cc.previous_stage_level,
    cc.previous_adjusted_score,
    'DELETED' as change_type,
    sm.adjusted_score - COALESCE(cc.previous_adjusted_score, 0) as score_impact
  FROM company_changes cc
  CROSS JOIN `{project}.{dataset}.hs_stage_mapping` sm
  WHERE cc.change_type = 'DELETED'
    AND sm.lifecycle_stage = 'disqualified'
    AND sm.lead_status IS NULL
    AND sm.deal_stage IS NULL
)
ORDER BY ABS(score_impact) DESC, change_type, owner_id, company_id
"""
}

# View 4: Pipeline History by Snapshot
VIEW_PIPELINE_HISTORY_BY_SNAPSHOT = {
    "name": "vw_pipeline_history_by_snapshot",
    "description": "Historical pipeline score trends by snapshot and owner",
    "sql": """
WITH owner_snapshot_scores AS (
  SELECT 
    pus.snapshot_id,
    pus.record_timestamp,
    pus.owner_id,
    o.email as owner_email,
    CONCAT(COALESCE(o.first_name, ''), ' ', COALESCE(o.last_name, '')) as owner_name,
    COUNT(*) as total_companies,
    SUM(pus.adjusted_score) as total_score,
    -- Score breakdown by stage level
    SUM(CASE WHEN pus.stage_level = -1 THEN pus.adjusted_score ELSE 0 END) as disqualified_score,
    SUM(CASE WHEN pus.stage_level = 0 THEN pus.adjusted_score ELSE 0 END) as nurturing_score,
    SUM(CASE WHEN pus.stage_level BETWEEN 1 AND 3 THEN pus.adjusted_score ELSE 0 END) as lead_score,
    SUM(CASE WHEN pus.stage_level = 4 THEN pus.adjusted_score ELSE 0 END) as sql_score,
    SUM(CASE WHEN pus.stage_level BETWEEN 5 AND 8 THEN pus.adjusted_score ELSE 0 END) as opportunity_score,
    SUM(CASE WHEN pus.stage_level = 9 THEN pus.adjusted_score ELSE 0 END) as closed_won_score,
    -- Company count breakdown by stage level
    SUM(CASE WHEN pus.stage_level = -1 THEN 1 ELSE 0 END) as disqualified_count,
    SUM(CASE WHEN pus.stage_level = 0 THEN 1 ELSE 0 END) as nurturing_count,
    SUM(CASE WHEN pus.stage_level BETWEEN 1 AND 3 THEN 1 ELSE 0 END) as lead_count,
    SUM(CASE WHEN pus.stage_level = 4 THEN 1 ELSE 0 END) as sql_count,
    SUM(CASE WHEN pus.stage_level BETWEEN 5 AND 8 THEN 1 ELSE 0 END) as opportunity_count,
    SUM(CASE WHEN pus.stage_level = 9 THEN 1 ELSE 0 END) as closed_won_count
  FROM `{project}.{dataset}.hs_pipeline_units_snapshot` pus
  LEFT JOIN `{project}.{dataset}.hs_owners` o ON pus.owner_id = o.owner_id
  GROUP BY 1,2,3,4,5
),
snapshot_totals AS (
  SELECT 
    snapshot_id,
    record_timestamp,
    COUNT(DISTINCT owner_id) as active_owners,
    SUM(total_companies) as total_companies_all_owners,
    SUM(total_score) as total_score_all_owners,
    AVG(total_score) as avg_score_per_owner
  FROM owner_snapshot_scores
  GROUP BY 1,2
)
SELECT 
  oss.*,
  st.active_owners,
  st.total_companies_all_owners,
  st.total_score_all_owners,
  st.avg_score_per_owner,
  -- Add ranking within each snapshot
  ROW_NUMBER() OVER (PARTITION BY oss.snapshot_id ORDER BY oss.total_score DESC) as owner_rank_in_snapshot,
  -- Add period-over-period calculation (previous snapshot for same owner)
  LAG(oss.total_score) OVER (PARTITION BY oss.owner_id ORDER BY oss.record_timestamp) as previous_total_score,
  LAG(oss.total_companies) OVER (PARTITION BY oss.owner_id ORDER BY oss.record_timestamp) as previous_total_companies,
  LAG(oss.record_timestamp) OVER (PARTITION BY oss.owner_id ORDER BY oss.record_timestamp) as previous_record_timestamp,
  -- Calculate change from previous snapshot
  oss.total_score - LAG(oss.total_score) OVER (PARTITION BY oss.owner_id ORDER BY oss.record_timestamp) as score_change_from_previous,
  oss.total_companies - LAG(oss.total_companies) OVER (PARTITION BY oss.owner_id ORDER BY oss.record_timestamp) as company_change_from_previous
FROM owner_snapshot_scores oss
JOIN snapshot_totals st ON oss.snapshot_id = st.snapshot_id
ORDER BY oss.record_timestamp DESC, oss.total_score DESC, oss.owner_id
"""
}

def get_all_view_definitions() -> Dict[str, Dict[str, Any]]:
    """Get all view definitions for creation/update"""
    return {
        VIEW_CURRENT_PIPELINE_BY_OWNER["name"]: VIEW_CURRENT_PIPELINE_BY_OWNER,
        VIEW_PIPELINE_COMPARISON["name"]: VIEW_PIPELINE_COMPARISON,
        VIEW_PIPELINE_CHANGES["name"]: VIEW_PIPELINE_CHANGES,
        VIEW_PIPELINE_HISTORY_BY_SNAPSHOT["name"]: VIEW_PIPELINE_HISTORY_BY_SNAPSHOT,
    }