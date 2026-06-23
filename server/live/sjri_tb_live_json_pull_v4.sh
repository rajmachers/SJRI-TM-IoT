#!/usr/bin/env bash
set -euo pipefail

TB_HOST="${TB_HOST:-64.227.171.244}"
TB_USER="${TB_USER:-root}"
TB_PASS="${TB_PASS:-}"
TB_BASE_PATH="${TB_BASE_PATH:-/opt/iot-platform/docker}"
OUT_FILE="${OUT_FILE:-scripts/output/v4_live_data.json}"
RUN_LOCAL_DOCKER="${RUN_LOCAL_DOCKER:-false}"
TB_CONTAINER="${TB_CONTAINER:-christ}"

if [[ "$RUN_LOCAL_DOCKER" != "true" && -z "$TB_PASS" ]]; then
  echo "ERROR: TB_PASS is required" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUT_FILE")"

read -r -d '' SQL <<'EOF' || true
WITH now_ctx AS (
  SELECT now() AT TIME ZONE 'Asia/Kolkata' AS now_ist
),
devices AS (
  SELECT id, COALESCE(NULLIF(label,''), name) AS asset_label
  FROM device
  WHERE type='PoC-Tag'
),
asset_latest_keys AS (
  SELECT
    t.entity_id,
    k.key,
    COALESCE(t.str_v, t.long_v::text, t.dbl_v::text, t.json_v::text) AS value_text,
    t.ts
  FROM ts_kv_latest t
  JOIN key_dictionary k ON k.key_id=t.key
  JOIN devices d ON d.id=t.entity_id
  WHERE k.key IN (
    'device_type','current_location_name','location_name','visit_start_time',
    'trac_timestamp','track_timestamp','last_tracked','daily_usage_hours','location_visit_duration',
    'total_visits','movement_status','fence_violation'
  )
),
asset_latest AS (
  SELECT
    d.id,
    d.asset_label,
    MAX(CASE WHEN k.key='device_type' THEN k.value_text END) AS device_type,
    MAX(CASE WHEN k.key IN ('current_location_name','location_name') THEN k.value_text END) AS current_location,
    MAX(CASE WHEN k.key='visit_start_time' THEN k.value_text END) AS arrived_at_ist,
    MAX(CASE WHEN k.key IN ('trac_timestamp','track_timestamp','last_tracked') THEN k.value_text END) AS last_tracked_ist,
    MAX(CASE WHEN k.key='daily_usage_hours' THEN NULLIF(regexp_replace(k.value_text, '[^0-9\\.\\-]', '', 'g'), '')::numeric END) AS daily_usage_hours,
    MAX(CASE WHEN k.key='location_visit_duration' THEN NULLIF(regexp_replace(k.value_text, '[^0-9\\.\\-]', '', 'g'), '')::numeric END) AS location_visit_duration,
    MAX(CASE WHEN k.key='total_visits' THEN NULLIF(regexp_replace(k.value_text, '[^0-9\\.\\-]', '', 'g'), '')::numeric END) AS total_visits,
    MAX(CASE WHEN k.key='movement_status' THEN k.value_text END) AS movement_status,
    MAX(CASE WHEN k.key='fence_violation' THEN lower(k.value_text) END) AS fence_violation,
    MAX(CASE WHEN k.key IN ('trac_timestamp','track_timestamp','last_tracked') THEN k.ts END) AS last_tracked_ts_key
  FROM devices d
  LEFT JOIN asset_latest_keys k ON k.entity_id=d.id
  GROUP BY d.id, d.asset_label
),
asset_last_seen AS (
  SELECT t.entity_id, MAX(t.ts) AS last_seen_ts
  FROM ts_kv t
  JOIN devices d ON d.id=t.entity_id
  GROUP BY t.entity_id
),
asset_state AS (
  SELECT
    al.id,
    al.asset_label,
    COALESCE(NULLIF(al.device_type,''), 'Unknown') AS device_type,
    COALESCE(NULLIF(al.current_location,''), 'Unknown') AS current_location,
    COALESCE(NULLIF(trim(al.arrived_at_ist),''), '-') AS arrived_at_ist,
    COALESCE(NULLIF(trim(al.last_tracked_ist),''), '-') AS last_tracked_ist,
    timezone('Asia/Kolkata', to_timestamp(COALESCE(al.last_tracked_ts_key, als.last_seen_ts)/1000.0)) AS last_seen_ts_ist,
    ROUND(((extract(epoch FROM now())*1000 - COALESCE(al.last_tracked_ts_key, als.last_seen_ts,0))/3600000.0)::numeric, 2) AS hours_since_seen,
    COALESCE(NULLIF(al.daily_usage_hours, 0), ROUND(COALESCE(al.location_visit_duration, 0) / 3600.0, 2), 0) AS daily_usage_hours,
    COALESCE(al.total_visits, 0) AS total_visits,
    COALESCE(NULLIF(al.movement_status,''), 'unknown') AS movement_status,
    COALESCE(al.fence_violation, 'false') AS fence_violation
  FROM asset_latest al
  LEFT JOIN asset_last_seen als ON als.entity_id=al.id
),
asset_util AS (
  SELECT
    a.*,
    GREATEST(MAX(a.total_visits) OVER (), 1) AS max_visits,
    ROUND(LEAST(100, (a.total_visits / GREATEST(MAX(a.total_visits) OVER (),1)) * 100), 1) AS utilization_rate_pct,
    CASE
      WHEN a.hours_since_seen > 24 THEN 'critical'
      WHEN a.hours_since_seen > 12 THEN 'high'
      WHEN a.hours_since_seen > 5 THEN 'moderate'
      ELSE 'healthy'
    END AS alert_level
  FROM asset_state a
),
ap_assets AS (
  SELECT
    a.id,
    COALESCE(NULLIF(a.label,''), a.name) AS location_label,
    p.name AS profile_name
  FROM asset a
  JOIN asset_profile p ON p.id = a.asset_profile_id
  WHERE p.name IN ('AccessPointsGF','AccessPointGF','AccessPointsF1','AccessPointsNB')
    AND COALESCE(NULLIF(a.label,''), a.name) <> '-NoDeviceName'
),
ap_latest AS (
  SELECT
    ap.id,
    ap.location_label,
    ap.profile_name,
    MAX(CASE WHEN k.key='current_device_count' THEN COALESCE(t.long_v, NULLIF(regexp_replace(t.str_v,'[^0-9\\.\\-]','','g'),'')::numeric) END) AS assets_now,
    MAX(CASE WHEN k.key='peak_occupancy_today' THEN COALESCE(t.long_v, NULLIF(regexp_replace(t.str_v,'[^0-9\\.\\-]','','g'),'')::numeric) END) AS peak_occupancy,
    MAX(CASE WHEN k.key='latitude' THEN COALESCE(t.dbl_v, NULLIF(t.str_v,'')::double precision) END) AS latitude,
    MAX(CASE WHEN k.key='longitude' THEN COALESCE(t.dbl_v, NULLIF(t.str_v,'')::double precision) END) AS longitude
  FROM ap_assets ap
  LEFT JOIN ts_kv_latest t ON t.entity_id=ap.id
  LEFT JOIN key_dictionary k ON k.key_id=t.key
  WHERE k.key IN ('current_device_count','peak_occupancy_today','latitude','longitude')
  GROUP BY ap.id, ap.location_label, ap.profile_name
),
zone_daily_poc AS (
  SELECT
    t.entity_id,
    date_trunc('day', to_timestamp(t.ts/1000.0) AT TIME ZONE 'Asia/Kolkata')::date AS d,
    MAX(COALESCE(t.long_v, NULLIF(regexp_replace(t.str_v,'[^0-9\\.\\-]','','g'),'')::numeric)) AS daily_total_visits
  FROM ts_kv t
  JOIN ap_assets ap ON ap.id=t.entity_id
  JOIN key_dictionary k ON k.key_id=t.key
  WHERE k.key='daily_total_visits'
    AND t.ts >= (extract(epoch FROM TIMESTAMP '2026-01-01 00:00:00')*1000)
  GROUP BY t.entity_id, date_trunc('day', to_timestamp(t.ts/1000.0) AT TIME ZONE 'Asia/Kolkata')::date
),
zone_overall AS (
  SELECT
    entity_id,
    ROUND(SUM(daily_total_visits)::numeric, 1) AS overall_visits,
    ROUND(AVG(daily_total_visits)::numeric, 1) AS avg_daily_visits_poc,
    COUNT(*)::bigint AS days_observed
  FROM zone_daily_poc
  GROUP BY entity_id
),
zone_rollup AS (
  SELECT ROUND(SUM(overall_visits)::numeric, 1) AS overall_visits_sum
  FROM zone_overall
),
zone_scored AS (
  SELECT
    z.id,
    z.location_label,
    z.profile_name,
    COALESCE(z.assets_now,0) AS assets_now,
    COALESCE(z.peak_occupancy,0) AS peak_occupancy,
    COALESCE(o.overall_visits,0) AS overall_visits,
    COALESCE(o.avg_daily_visits_poc,0) AS avg_daily_visits_poc,
    COALESCE(o.days_observed,0) AS days_observed,
    z.latitude,
    z.longitude,
    ROUND(
      CASE WHEN COALESCE((SELECT overall_visits_sum FROM zone_rollup),0) <= 0 THEN 0
      ELSE (COALESCE(o.overall_visits,0) / (SELECT overall_visits_sum FROM zone_rollup)) * 100
      END
    ,1) AS demand_share_pct,
    ROUND((0.5*COALESCE(o.avg_daily_visits_poc,0) + 0.3*COALESCE(z.assets_now,0) + 0.2*COALESCE(z.peak_occupancy,0))::numeric, 1) AS pressure_score
  FROM ap_latest z
  LEFT JOIN zone_overall o ON o.entity_id=z.id
  WHERE z.location_label NOT ILIKE 'Unknown%'
    AND z.location_label <> '-NoDeviceName'
    AND COALESCE(o.overall_visits,0) > 0
),
top3_zones AS (
  SELECT *
  FROM zone_scored
  ORDER BY pressure_score DESC, overall_visits DESC, location_label
  LIMIT 3
),
unauth_events_24h AS (
  SELECT COUNT(*)::bigint AS cnt
  FROM ts_kv t
  JOIN devices d ON d.id=t.entity_id
  JOIN key_dictionary k ON k.key_id=t.key
  WHERE k.key='fence_violation'
    AND t.ts >= (extract(epoch FROM now() - interval '24 hour')*1000)
    AND lower(COALESCE(t.str_v, t.long_v::text, t.dbl_v::text, t.json_v::text, 'false')) IN ('true','1','yes')
),
kpis AS (
  SELECT
    COUNT(*)::bigint AS tracked_assets,
    ROUND(AVG(utilization_rate_pct)::numeric,1) AS utilization_rate_pct,
    COUNT(*) FILTER (WHERE hours_since_seen > 12)::bigint AS idle_gt_12h_count,
    COUNT(*) FILTER (WHERE hours_since_seen > 5)::bigint AS last_seen_alerts_gt_5h,
    COUNT(*) FILTER (WHERE (current_location ILIKE 'Unknown%' OR current_location='-NoDeviceName') AND hours_since_seen > 12)::bigint AS unaccounted_gt_12h_count,
    (SELECT cnt FROM unauth_events_24h) AS unauthorized_events_24h
  FROM asset_util
),
visits_rollup AS (
  SELECT
    ROUND(SUM(overall_visits)::numeric,1) AS total_visits_overall_sum,
    ROUND(SUM(avg_daily_visits_poc)::numeric,1) AS avg_daily_visits_poc_sum
  FROM zone_scored
)
SELECT json_build_object(
  'generated_at_ist', to_char((SELECT now_ist FROM now_ctx), 'YYYY-MM-DD HH24:MI:SS') || ' IST',
  'refresh_interval_sec', 120,
  'thresholds', json_build_object('idle_hours', 12, 'last_seen_alert_hours', 5),
  'kpis', json_build_object(
    'tracked_assets', (SELECT tracked_assets FROM kpis),
    'utilization_rate_pct', (SELECT utilization_rate_pct FROM kpis),
    'idle_gt_12h_count', (SELECT idle_gt_12h_count FROM kpis),
    'last_seen_alerts_gt_5h', (SELECT last_seen_alerts_gt_5h FROM kpis),
    'unaccounted_gt_12h_count', (SELECT unaccounted_gt_12h_count FROM kpis),
    'unauthorized_events_24h', (SELECT unauthorized_events_24h FROM kpis),
    'total_visits_overall_sum', (SELECT total_visits_overall_sum FROM visits_rollup),
    'avg_daily_visits_poc_sum', (SELECT avg_daily_visits_poc_sum FROM visits_rollup)
  ),
  'asset_utilization', (
    SELECT COALESCE(json_agg(json_build_object(
      'asset_label', asset_label,
      'device_type', device_type,
      'current_location', current_location,
      'arrived_at_ist', arrived_at_ist,
      'last_tracked_ist', last_tracked_ist,
      'hours_since_seen', hours_since_seen,
      'daily_usage_hours', daily_usage_hours,
      'total_visits', total_visits,
      'movement_status', movement_status,
      'utilization_rate_pct', utilization_rate_pct,
      'alert_level', alert_level
    ) ORDER BY utilization_rate_pct DESC, asset_label), '[]'::json)
    FROM asset_util
  ),
  'idle_alerts', (
    SELECT COALESCE(json_agg(json_build_object(
      'asset_label', asset_label,
      'current_location', current_location,
      'last_tracked_ist', last_tracked_ist,
      'hours_since_seen', hours_since_seen,
      'alert_level', alert_level
    ) ORDER BY hours_since_seen DESC, asset_label), '[]'::json)
    FROM asset_util
    WHERE hours_since_seen > 5
  ),
  'zone_demand', (
    SELECT COALESCE(json_agg(json_build_object(
      'location_label', location_label,
      'profile_name', profile_name,
      'assets_now', assets_now,
      'peak_occupancy', peak_occupancy,
      'overall_visits', overall_visits,
      'avg_daily_visits_poc', avg_daily_visits_poc,
      'days_observed', days_observed,
      'demand_share_pct', demand_share_pct,
      'pressure_score', pressure_score,
      'latitude', latitude,
      'longitude', longitude
    ) ORDER BY pressure_score DESC, location_label), '[]'::json)
    FROM zone_scored
  ),
  'top3_high_demand_zones', (
    SELECT COALESCE(json_agg(json_build_object(
      'location_label', location_label,
      'overall_visits', overall_visits,
      'avg_daily_visits_poc', avg_daily_visits_poc,
      'demand_share_pct', demand_share_pct,
      'assets_now', assets_now,
      'pressure_score', pressure_score
    ) ORDER BY pressure_score DESC, location_label), '[]'::json)
    FROM top3_zones
  ),
  'exceptions', json_build_object(
    'unaccounted_assets', (
      SELECT COALESCE(json_agg(json_build_object(
        'asset_label', asset_label,
        'current_location', current_location,
        'hours_since_seen', hours_since_seen,
        'last_tracked_ist', last_tracked_ist
      ) ORDER BY hours_since_seen DESC, asset_label), '[]'::json)
      FROM asset_util
      WHERE (current_location ILIKE 'Unknown%' OR current_location='-NoDeviceName')
    ),
    'unauthorized_events_24h', (SELECT cnt FROM unauth_events_24h)
  ),
  'notes', json_build_object(
    'unauthorized_zone_proxy', 'Uses fence_violation telemetry as proxy until restricted-zone master is provided.',
    'maintenance_model_status', 'Usage-based maintenance model pending OEM threshold master and service logs.'
  )
) AS payload;
EOF

if [[ "$RUN_LOCAL_DOCKER" == "true" ]]; then
  JSON_OUT=$(printf "%s\n" "$SQL" | docker exec -i "$TB_CONTAINER" psql -U thingsboard -d thingsboard -t -A | tail -n 1)
else
  JSON_OUT=$(printf "%s\n" "$SQL" | sshpass -p "$TB_PASS" ssh -o StrictHostKeyChecking=no "${TB_USER}@${TB_HOST}" \
    "su - iotuser -c 'cd ${TB_BASE_PATH} && docker exec -i ${TB_CONTAINER} psql -U thingsboard -d thingsboard -t -A'" | tail -n 1)
fi

printf "%s\n" "$JSON_OUT" > "$OUT_FILE"

echo "Saved v4 JSON to: $OUT_FILE"
