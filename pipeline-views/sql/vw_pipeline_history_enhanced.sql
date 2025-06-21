-- Enhanced Pipeline History View for Looker Studio
-- Compatible with current HubSpot Pipeline schema
-- Adds company_type and deal_type for filtering
-- Excludes disqualified and closed_won stages for active pipeline view

CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET_ID}.vw_pipeline_history_enhanced` AS

WITH pipeline_units_with_details AS (
  SELECT 
    pus.snapshot_id,
    pus.record_timestamp,
    pus.owner_id,
    pus.company_id,
    pus.deal_id,
    pus.combined_stage,
    pus.stage_level,
    pus.adjusted_score,
    -- Get company details
    c.company_type,
    c.company_name,
    c.development_category,
    c.hiring_developers,
    c.inhouse_developers,
    -- Get deal details
    d.deal_type,
    d.deal_name,
    d.amount as deal_amount,
    -- Get owner details
    o.email as owner_email,
    CONCAT(COALESCE(o.first_name, ''), ' ', COALESCE(o.last_name, '')) as owner_name
  FROM `${PROJECT_ID}.${DATASET_ID}.hs_pipeline_units_snapshot` pus
  LEFT JOIN `${PROJECT_ID}.${DATASET_ID}.hs_companies` c 
    ON pus.company_id = c.company_id 
    AND pus.snapshot_id = c.snapshot_id  -- Match snapshot for historical accuracy
  LEFT JOIN `${PROJECT_ID}.${DATASET_ID}.hs_deals` d 
    ON pus.deal_id = d.deal_id 
    AND pus.snapshot_id = d.snapshot_id  -- Match snapshot for historical accuracy
  LEFT JOIN `${PROJECT_ID}.${DATASET_ID}.hs_owners` o 
    ON pus.owner_id = o.owner_id
  -- Filter out disqualified and closed stages for active pipeline view
  WHERE pus.stage_level NOT IN (-1, 9)  -- Exclude disqualified (-1) and closed_won (9)
),

owner_snapshot_aggregates AS (
  SELECT 
    snapshot_id,
    record_timestamp,
    owner_id,
    owner_email,
    owner_name,
    
    -- Company type breakdown for filtering
    STRING_AGG(DISTINCT company_type ORDER BY company_type) as company_types,
    STRING_AGG(DISTINCT deal_type ORDER BY deal_type) as deal_types,
    STRING_AGG(DISTINCT development_category ORDER BY development_category) as development_categories,
    STRING_AGG(DISTINCT hiring_developers ORDER BY hiring_developers) as hiring_status,
    
    -- Overall metrics
    COUNT(DISTINCT company_id) as total_companies,
    COUNT(DISTINCT deal_id) as total_deals,
    SUM(adjusted_score) as total_score,
    AVG(adjusted_score) as avg_score_per_unit,
    SUM(COALESCE(deal_amount, 0)) as total_deal_value,
    
    -- Stage-level breakdowns (counts)
    SUM(CASE WHEN stage_level = 0 THEN 1 ELSE 0 END) as nurturing_count,
    SUM(CASE WHEN stage_level BETWEEN 1 AND 3 THEN 1 ELSE 0 END) as lead_count,
    SUM(CASE WHEN stage_level = 4 THEN 1 ELSE 0 END) as sql_count,
    SUM(CASE WHEN stage_level BETWEEN 5 AND 8 THEN 1 ELSE 0 END) as opportunity_count,
    
    -- Stage-level breakdowns (scores)
    SUM(CASE WHEN stage_level = 0 THEN adjusted_score ELSE 0 END) as nurturing_score,
    SUM(CASE WHEN stage_level BETWEEN 1 AND 3 THEN adjusted_score ELSE 0 END) as lead_score,
    SUM(CASE WHEN stage_level = 4 THEN adjusted_score ELSE 0 END) as sql_score,
    SUM(CASE WHEN stage_level BETWEEN 5 AND 8 THEN adjusted_score ELSE 0 END) as opportunity_score,
    
    -- Company type counts for additional insights
    SUM(CASE WHEN company_type = 'Prospect' THEN 1 ELSE 0 END) as prospect_count,
    SUM(CASE WHEN company_type = 'Customer' THEN 1 ELSE 0 END) as customer_count,
    SUM(CASE WHEN company_type = 'Partner' THEN 1 ELSE 0 END) as partner_count,
    SUM(CASE WHEN company_type = 'Vendor' THEN 1 ELSE 0 END) as vendor_count,
    
    -- Deal type counts
    SUM(CASE WHEN deal_type IS NOT NULL THEN 1 ELSE 0 END) as companies_with_deals,
    SUM(CASE WHEN deal_type = 'New Business' THEN 1 ELSE 0 END) as new_business_count,
    SUM(CASE WHEN deal_type = 'Existing Business' THEN 1 ELSE 0 END) as existing_business_count
    
  FROM pipeline_units_with_details
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
  FROM owner_snapshot_aggregates
  GROUP BY 1,2
)

SELECT 
  osa.*,
  st.active_owners,
  st.total_companies_all_owners,
  st.total_score_all_owners,
  st.avg_score_per_owner,
  
  -- Add ranking within each snapshot
  ROW_NUMBER() OVER (PARTITION BY osa.snapshot_id ORDER BY osa.total_score DESC) as owner_rank_in_snapshot,
  
  -- Add period-over-period calculation (previous snapshot for same owner)
  LAG(osa.total_score) OVER (PARTITION BY osa.owner_id ORDER BY osa.record_timestamp) as previous_total_score,
  LAG(osa.total_companies) OVER (PARTITION BY osa.owner_id ORDER BY osa.record_timestamp) as previous_total_companies,
  LAG(osa.record_timestamp) OVER (PARTITION BY osa.owner_id ORDER BY osa.record_timestamp) as previous_record_timestamp,
  
  -- Calculate change from previous snapshot
  osa.total_score - LAG(osa.total_score) OVER (PARTITION BY osa.owner_id ORDER BY osa.record_timestamp) as score_change_from_previous,
  osa.total_companies - LAG(osa.total_companies) OVER (PARTITION BY osa.owner_id ORDER BY osa.record_timestamp) as company_change_from_previous,
  
  -- Percentage change
  CASE 
    WHEN LAG(osa.total_score) OVER (PARTITION BY osa.owner_id ORDER BY osa.record_timestamp) = 0 THEN NULL
    ELSE ROUND(
      (osa.total_score - LAG(osa.total_score) OVER (PARTITION BY osa.owner_id ORDER BY osa.record_timestamp)) 
      / LAG(osa.total_score) OVER (PARTITION BY osa.owner_id ORDER BY osa.record_timestamp) * 100, 2
    )
  END as score_change_percent

FROM owner_snapshot_aggregates osa
JOIN snapshot_totals st ON osa.snapshot_id = st.snapshot_id
ORDER BY osa.record_timestamp DESC, osa.total_score DESC, osa.owner_id;