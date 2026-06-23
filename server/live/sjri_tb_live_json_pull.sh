#!/usr/bin/env bash
set -euo pipefail

TB_HOST="${TB_HOST:-64.227.171.244}"
TB_USER="${TB_USER:-root}"
TB_PASS="${TB_PASS:-}"
TB_BASE_PATH="${TB_BASE_PATH:-/opt/iot-platform/docker}"
OUT_FILE="${OUT_FILE:-scripts/output/sjri_live_data.json}"
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
  SELECT id, name, COALESCE(NULLIF(label,''), name) AS asset_label
  FROM device
  WHERE type='PoC-Tag'
),
ap_assets AS (
  SELECT
    a.id,
    COALESCE(NULLIF(a.label,''), a.name) AS location_label,
    ap.name AS profile_name
  FROM asset a
  JOIN asset_profile ap ON ap.id = a.asset_profile_id
  WHERE ap.name IN ('AccessPointsGF','AccessPointGF','AccessPointsF1','AccessPointsNB')
    AND COALESCE(NULLIF(a.label,''), a.name) <> '-NoDeviceName'
),
latest_keys AS (
  SELECT
    t.entity_id,
    k.key,
    COALESCE(t.str_v, t.long_v::text, t.dbl_v::text, t.json_v::text) AS value_text,
    t.ts
  FROM ts_kv_latest t
  JOIN key_dictionary k ON k.key_id=t.key
  WHERE k.key IN (
    'device_type',
    'current_location_name',
    'location_name',
    'visit_start_time',
    'trac_timestamp',
    'track_timestamp',
    'last_tracked',
    'arrived_stale',
    'last_tracked_stale',
    'arrived_stale_message',
    'last_tracked_stale_message'
  )
),
device_latest AS (
  SELECT
    d.id,
    d.asset_label,
    MAX(CASE WHEN lk.key='device_type' THEN lk.value_text END) AS device_type,
    MAX(CASE WHEN lk.key IN ('current_location_name','location_name') THEN lk.value_text END) AS current_location,
    MAX(CASE WHEN lk.key='visit_start_time' THEN lk.value_text END) AS visit_start_raw,
    MAX(CASE WHEN lk.key IN ('trac_timestamp','track_timestamp','last_tracked') THEN lk.value_text END) AS last_tracked_raw,
    MAX(CASE WHEN lk.key='arrived_stale' THEN lk.value_text END) AS arrived_stale_raw,
    MAX(CASE WHEN lk.key='last_tracked_stale' THEN lk.value_text END) AS last_tracked_stale_raw,
    MAX(CASE WHEN lk.key='arrived_stale_message' THEN lk.value_text END) AS arrived_stale_msg,
    MAX(CASE WHEN lk.key='last_tracked_stale_message' THEN lk.value_text END) AS last_tracked_stale_msg,
    MAX(CASE WHEN lk.key='visit_start_time' THEN lk.ts END) AS visit_start_ts,
    MAX(CASE WHEN lk.key IN ('trac_timestamp','track_timestamp','last_tracked') THEN lk.ts END) AS last_tracked_ts_key
  FROM devices d
  LEFT JOIN latest_keys lk ON lk.entity_id=d.id
  GROUP BY d.id, d.asset_label
),
device_last_seen AS (
  SELECT t.entity_id, MAX(t.ts) AS last_seen_ts
  FROM ts_kv t
  JOIN devices d ON d.id=t.entity_id
  GROUP BY t.entity_id
),
device_rows AS (
  SELECT
    dl.id,
    dl.asset_label,
    COALESCE(NULLIF(dl.device_type,''), 'Unknown') AS device_type,
    COALESCE(NULLIF(dl.current_location,''), 'Unknown') AS current_location,
    COALESCE(NULLIF(trim(dl.visit_start_raw),''), '-') AS arrived_at_ist,
    COALESCE(NULLIF(trim(dl.last_tracked_raw),''), '-') AS last_tracked_ist,
    timezone('Asia/Kolkata', to_timestamp(COALESCE(dl.last_tracked_ts_key, ds.last_seen_ts)/1000.0)) AS last_tracked_sort_ts,
    ROUND(((extract(epoch FROM now())*1000 - COALESCE(dl.last_tracked_ts_key, ds.last_seen_ts,0))/3600000.0)::numeric, 2) AS hours_since_seen,
    CASE
      WHEN ((extract(epoch FROM now())*1000 - COALESCE(dl.last_tracked_ts_key, ds.last_seen_ts,0))/3600000.0) >= 24 THEN 'critical'
      WHEN ((extract(epoch FROM now())*1000 - COALESCE(dl.last_tracked_ts_key, ds.last_seen_ts,0))/3600000.0) >= 12 THEN 'high'
      WHEN ((extract(epoch FROM now())*1000 - COALESCE(dl.last_tracked_ts_key, ds.last_seen_ts,0))/3600000.0) >= 5 THEN 'moderate'
      ELSE 'none'
    END AS stale_level,
    CASE
      WHEN ((extract(epoch FROM now())*1000 - COALESCE(dl.last_tracked_ts_key, ds.last_seen_ts,0))/3600000.0) < 5
        THEN 'Healthy: ' || ROUND(((extract(epoch FROM now())*1000 - COALESCE(dl.last_tracked_ts_key, ds.last_seen_ts,0))/3600000.0)::numeric, 1) || 'h delay'
      WHEN ((extract(epoch FROM now())*1000 - COALESCE(dl.last_tracked_ts_key, ds.last_seen_ts,0))/3600000.0) < 12
        THEN 'Watch: ' || ROUND(((extract(epoch FROM now())*1000 - COALESCE(dl.last_tracked_ts_key, ds.last_seen_ts,0))/3600000.0)::numeric, 1) || 'h since last seen'
      WHEN ((extract(epoch FROM now())*1000 - COALESCE(dl.last_tracked_ts_key, ds.last_seen_ts,0))/3600000.0) < 24
        THEN 'Stale: ' || ROUND(((extract(epoch FROM now())*1000 - COALESCE(dl.last_tracked_ts_key, ds.last_seen_ts,0))/3600000.0)::numeric, 1) || 'h since last seen'
      ELSE 'Critical: ' || GREATEST(1, FLOOR(((extract(epoch FROM now())*1000 - COALESCE(dl.last_tracked_ts_key, ds.last_seen_ts,0))/3600000.0) / 24.0)::int)
           || ' ' || CASE WHEN FLOOR(((extract(epoch FROM now())*1000 - COALESCE(dl.last_tracked_ts_key, ds.last_seen_ts,0))/3600000.0) / 24.0)::int = 1 THEN 'day' ELSE 'days' END || ' delay'
    END AS stale_message
  FROM device_latest dl
  LEFT JOIN device_last_seen ds ON ds.entity_id=dl.id
),
location_metrics AS (
  SELECT
    COALESCE(NULLIF(aa.location_label,''), MAX(CASE WHEN k.key='location_name' THEN COALESCE(t.str_v,t.json_v::text) END), 'Unknown') AS location_label,
    aa.profile_name,
    COALESCE(MAX(CASE WHEN k.key='current_device_count' THEN COALESCE(t.long_v, NULLIF(regexp_replace(t.str_v, '[^0-9]', '', 'g'), '')::bigint) END), 0) AS assets_now,
    COALESCE(MAX(CASE WHEN k.key='peak_occupancy_today' THEN COALESCE(t.long_v, NULLIF(regexp_replace(t.str_v, '[^0-9]', '', 'g'), '')::bigint) END), 0) AS peak_occupancy,
    COALESCE(MAX(CASE WHEN k.key='daily_total_visits' THEN COALESCE(t.long_v, NULLIF(regexp_replace(t.str_v, '[^0-9]', '', 'g'), '')::bigint) END), 0) AS total_visits_today,
    COALESCE(MAX(CASE WHEN k.key='device_names_present' THEN COALESCE(t.str_v, t.json_v::text) END), '') AS device_names_present,
    COALESCE(MAX(CASE WHEN k.key='device_types_present' THEN COALESCE(t.str_v, t.json_v::text) END), '') AS device_types_present
  FROM ts_kv_latest t
  JOIN ap_assets aa ON aa.id=t.entity_id
  JOIN key_dictionary k ON k.key_id=t.key
  WHERE k.key IN ('location_name','current_device_count','peak_occupancy_today','daily_total_visits','device_names_present','device_types_present')
  GROUP BY t.entity_id, aa.location_label, aa.profile_name
),
location_top AS (
  SELECT *
  FROM location_metrics
  ORDER BY total_visits_today DESC, peak_occupancy DESC, assets_now DESC, location_label
  LIMIT 8
),
today_location_utilization AS (
  SELECT
    lm.location_label,
    COALESCE(lm.profile_name, 'Unmapped') AS profile_name,
    COALESCE(lm.assets_now, 0) AS assets_now,
    COALESCE(lm.peak_occupancy, 0) AS peak_occupancy,
    COALESCE(lm.total_visits_today, 0) AS total_visits_today,
    COALESCE(lm.device_names_present, '') AS device_names_present,
    COALESCE(lm.device_types_present, '') AS device_types_present
  FROM location_metrics lm
  WHERE lm.location_label NOT ILIKE 'Unknown%'
    AND lm.location_label <> '-NoDeviceName'
    AND COALESCE(lm.total_visits_today,0) > 0
  ORDER BY lm.total_visits_today DESC, lm.peak_occupancy DESC, lm.assets_now DESC, lm.location_label
  LIMIT 8
),
mix AS (
  SELECT COALESCE(NULLIF(device_type,''), 'Unknown') AS asset_type, COUNT(*) AS cnt
  FROM device_latest
  GROUP BY 1
),
device_activity AS (
  SELECT
    d.asset_label,
    COUNT(*) AS activity_events,
    COUNT(DISTINCT date_trunc('day', to_timestamp(t.ts/1000.0) AT TIME ZONE 'Asia/Kolkata')) AS active_days
  FROM ts_kv t
  JOIN devices d ON d.id=t.entity_id
  GROUP BY d.asset_label
),
top_assets_activity AS (
  SELECT
    asset_label,
    activity_events,
    active_days,
    CASE
      WHEN activity_events >= 300000 THEN 'Very High'
      WHEN activity_events >= 250000 THEN 'High'
      WHEN activity_events >= 150000 THEN 'Medium'
      ELSE 'Low'
    END AS use_level
  FROM device_activity
  ORDER BY activity_events DESC, asset_label
  LIMIT 5
),
known_location_activity AS (
  SELECT
    COALESCE(NULLIF(trim(t.str_v),''), 'Unknown') AS location_label,
    COUNT(*)::bigint AS activity_events
  FROM ts_kv t
  JOIN devices d ON d.id=t.entity_id
  JOIN key_dictionary k ON k.key_id=t.key
  WHERE k.key IN ('location_name','current_location_name')
    AND t.ts >= extract(epoch FROM timestamp '2026-01-01 00:00:00+00')*1000
  GROUP BY COALESCE(NULLIF(trim(t.str_v),''), 'Unknown')
),
top_known_locations AS (
  SELECT location_label, activity_events
  FROM known_location_activity
  WHERE location_label NOT ILIKE 'Unknown%'
    AND location_label <> '-NoDeviceName'
  ORDER BY activity_events DESC, location_label
  LIMIT 5
),
swap_map AS (
  SELECT * FROM (VALUES
    ('Wheelchair 21','Pulseoximeter - ME241099'),
    ('Wheelchair 32','Pulseoximeter - ME21791'),
    ('Wheelchair 25','Pulseoximeter - ME21790'),
    ('Wheelchair 16','Pulseoximeter - ME240200'),
    ('Wheelchair 4','Pulseoximeter - ME21788')
  ) AS m(pre_swap_wheelchair, post_swap_asset)
),
device_id_map AS (
  SELECT id, asset_label
  FROM devices
),
swap_raw AS (
  SELECT
    sm.pre_swap_wheelchair,
    sm.post_swap_asset,
    dm.id AS device_id
  FROM swap_map sm
  LEFT JOIN device_id_map dm ON dm.asset_label = sm.post_swap_asset
),
swap_stats AS (
  SELECT
    sr.pre_swap_wheelchair,
    sr.post_swap_asset,
    COALESCE(SUM(CASE WHEN t.ts BETWEEN extract(epoch FROM timestamp '2026-01-01 00:00:00+00')*1000
                              AND extract(epoch FROM timestamp '2026-03-02 23:59:59+00')*1000 THEN 1 ELSE 0 END),0) AS pre_points,
    COALESCE(SUM(CASE WHEN t.ts >= extract(epoch FROM timestamp '2026-03-03 00:00:00+00')*1000 THEN 1 ELSE 0 END),0) AS post_points
  FROM swap_raw sr
  LEFT JOIN ts_kv t ON t.entity_id=sr.device_id
  GROUP BY sr.pre_swap_wheelchair, sr.post_swap_asset
),
swap_impact AS (
  SELECT
    pre_swap_wheelchair,
    post_swap_asset,
    ROUND((pre_points / GREATEST((extract(epoch FROM timestamp '2026-03-02 23:59:59+00') - extract(epoch FROM timestamp '2026-01-01 00:00:00+00'))/86400.0, 1))::numeric, 0) AS pre_swap_daily_avg,
    ROUND((post_points / GREATEST((extract(epoch FROM now()) - extract(epoch FROM timestamp '2026-03-03 00:00:00+00'))/86400.0, 1))::numeric, 0) AS post_swap_daily_avg,
    ROUND(
      CASE
        WHEN pre_points = 0 THEN NULL
        ELSE (
          (
            post_points / GREATEST((extract(epoch FROM now()) - extract(epoch FROM timestamp '2026-03-03 00:00:00+00'))/86400.0, 1)
          ) - (
            pre_points / GREATEST((extract(epoch FROM timestamp '2026-03-02 23:59:59+00') - extract(epoch FROM timestamp '2026-01-01 00:00:00+00'))/86400.0, 1)
          )
        ) /
        (
          pre_points / GREATEST((extract(epoch FROM timestamp '2026-03-02 23:59:59+00') - extract(epoch FROM timestamp '2026-01-01 00:00:00+00'))/86400.0, 1)
        ) * 100
      END
    ::numeric, 1) AS increase_pct
  FROM swap_stats
),
today_location_activity AS (
  SELECT
    COALESCE(NULLIF(trim(t.str_v),''), 'Unknown') AS location_label,
    COUNT(*)::bigint AS activity_events
  FROM ts_kv t
  JOIN devices d ON d.id=t.entity_id
  JOIN key_dictionary k ON k.key_id=t.key
  WHERE k.key IN ('location_name','current_location_name')
    AND t.ts >= extract(epoch FROM date_trunc('day', now()))*1000
  GROUP BY COALESCE(NULLIF(trim(t.str_v),''), 'Unknown')
),
top_locations_active_use AS (
  SELECT
    location_label,
    activity_events,
    CASE
      WHEN activity_events >= 250 THEN 'Very High'
      WHEN activity_events >= 120 THEN 'High'
      WHEN activity_events >= 40 THEN 'Medium'
      ELSE 'Low'
    END AS utilization_band
  FROM today_location_activity
  WHERE location_label NOT ILIKE 'Unknown%'
    AND location_label <> '-NoDeviceName'
  ORDER BY activity_events DESC, location_label
  LIMIT 5
),
topline AS (
  SELECT
    (SELECT COUNT(*) FROM devices) AS assets_reporting,
    (SELECT COUNT(*) FROM ap_assets WHERE location_label NOT ILIKE 'Unknown%') AS known_locations_covered,
    (SELECT COUNT(*) FROM ap_assets WHERE profile_name IN ('AccessPointsGF','AccessPointGF')) AS ap_gf_count,
    (SELECT COUNT(*) FROM ap_assets WHERE profile_name='AccessPointsF1') AS ap_f1_count,
    (SELECT COUNT(*) FROM ap_assets WHERE profile_name='AccessPointsNB') AS ap_nb_count,
    (SELECT COUNT(*) FROM ts_kv t JOIN key_dictionary k ON k.key_id=t.key WHERE k.key='daily_location_visits' AND t.ts >= (extract(epoch FROM date_trunc('day', now()))*1000)) AS daily_location_visit_events,
    (SELECT COUNT(*) FROM ts_kv t JOIN key_dictionary k ON k.key_id=t.key WHERE k.key='current_location_name' AND COALESCE(t.str_v,'') ILIKE 'Unknown%' AND t.ts >= (extract(epoch FROM date_trunc('day', now()))*1000)) AS unknown_anchor_events
)
SELECT json_build_object(
  'generated_at_ist', to_char((SELECT now_ist FROM now_ctx), 'YYYY-MM-DD HH24:MI:SS') || ' IST',
  'kpis', json_build_object(
    'aruba_access_points_coverage', 157,
    'tracked_assets', (SELECT COUNT(*) FROM devices),
    'activity_events', (SELECT COUNT(*) FROM ts_kv t JOIN devices d ON d.id=t.entity_id)
  ),
  'asset_mix', (SELECT COALESCE(json_agg(json_build_object('asset_type', asset_type, 'count', cnt) ORDER BY cnt DESC, asset_type), '[]'::json) FROM mix),
  'topline', (SELECT row_to_json(topline) FROM topline),
  'today_asset_tracking', (
    SELECT COALESCE(json_agg(
      json_build_object(
        'asset_label', asset_label,
        'device_type', device_type,
        'current_location', current_location,
        'arrived_at_ist', arrived_at_ist,
        'last_tracked_ist', last_tracked_ist,
        'hours_since_seen', hours_since_seen,
        'stale_level', stale_level,
        'stale_message', stale_message
      )
      ORDER BY last_tracked_sort_ts DESC
    ), '[]'::json)
    FROM device_rows
  ),
  'today_location_utilization', (
    SELECT COALESCE(json_agg(
      json_build_object(
        'location_label', location_label,
        'profile_name', profile_name,
        'assets_now', assets_now,
        'peak_occupancy', peak_occupancy,
        'total_visits_today', total_visits_today,
        'device_names_present', device_names_present,
        'device_types_present', device_types_present
      )
      ORDER BY total_visits_today DESC, location_label
    ), '[]'::json)
    FROM today_location_utilization
  ),
  'top_assets_by_activity', (
    SELECT COALESCE(json_agg(
      json_build_object(
        'asset_label', asset_label,
        'activity_events', activity_events,
        'active_days', active_days,
        'use_level', use_level
      ) ORDER BY activity_events DESC
    ), '[]'::json)
    FROM top_assets_activity
  ),
  'top_known_locations_by_activity', (
    SELECT COALESCE(json_agg(
      json_build_object(
        'location_label', location_label,
        'activity_events', activity_events
      ) ORDER BY activity_events DESC
    ), '[]'::json)
    FROM top_known_locations
  ),
  'swap_impact_since_03_mar', (
    SELECT COALESCE(json_agg(
      json_build_object(
        'pre_swap_wheelchair', pre_swap_wheelchair,
        'post_swap_asset', post_swap_asset,
        'pre_swap_daily_avg', pre_swap_daily_avg,
        'post_swap_daily_avg', post_swap_daily_avg,
        'increase_pct', increase_pct
      ) ORDER BY increase_pct DESC NULLS LAST
    ), '[]'::json)
    FROM swap_impact
  ),
  'top_locations_in_active_use', (
    SELECT COALESCE(json_agg(
      json_build_object(
        'location_label', location_label,
        'activity_events', activity_events,
        'utilization_band', utilization_band
      ) ORDER BY activity_events DESC
    ), '[]'::json)
    FROM top_locations_active_use
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

echo "Saved live JSON to: $OUT_FILE"
