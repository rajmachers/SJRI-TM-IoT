#!/usr/bin/env bash
set -euo pipefail

TB_HOST="${TB_HOST:-64.227.171.244}"
TB_USER="${TB_USER:-root}"
TB_PASS="${TB_PASS:-}"
TB_BASE_PATH="${TB_BASE_PATH:-/opt/iot-platform/docker}"
OUT_FILE="${OUT_FILE:-scripts/output/sjri_live_data_v2.json}"
RUN_LOCAL_DOCKER="${RUN_LOCAL_DOCKER:-false}"
TB_CONTAINER="${TB_CONTAINER:-christ}"
BRIDGE_CONTAINER="${BRIDGE_CONTAINER:-christ_bridge}"

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
latest_location AS (
	SELECT
		t.entity_id,
		COALESCE(NULLIF(trim(MAX(CASE WHEN k.key IN ('current_location_name','location_name') THEN t.str_v END)),''), 'Unknown') AS current_location
	FROM ts_kv_latest t
	JOIN key_dictionary k ON k.key_id=t.key
	JOIN devices d ON d.id=t.entity_id
	WHERE k.key IN ('current_location_name','location_name')
	GROUP BY t.entity_id
),
last_seen AS (
	SELECT t.entity_id, MAX(t.ts) AS last_seen_ts
	FROM ts_kv t
	JOIN devices d ON d.id=t.entity_id
	GROUP BY t.entity_id
),
asset_state AS (
	SELECT
		d.id,
		d.asset_label,
		COALESCE(ll.current_location, 'Unknown') AS current_location,
		ls.last_seen_ts,
		((extract(epoch FROM now())*1000 - COALESCE(ls.last_seen_ts,0))/3600000.0) AS hours_since_seen,
		to_char(timezone('Asia/Kolkata', to_timestamp(COALESCE(ls.last_seen_ts,0)/1000.0)), 'YYYY-MM-DD HH24:MI:SS') || ' IST' AS last_seen_ist
	FROM devices d
	LEFT JOIN latest_location ll ON ll.entity_id=d.id
	LEFT JOIN last_seen ls ON ls.entity_id=d.id
),
unknown_assets AS (
	SELECT *
	FROM asset_state
	WHERE current_location ILIKE 'Unknown%' OR current_location='-NoDeviceName'
),
stale_assets AS (
	SELECT *
	FROM asset_state
	WHERE hours_since_seen > 5
),
loc_events_today AS (
	SELECT
		t.entity_id,
		COALESCE(NULLIF(trim(t.str_v),''), 'Unknown') AS location_label,
		d.asset_label,
		t.ts
	FROM ts_kv t
	JOIN devices d ON d.id=t.entity_id
	JOIN key_dictionary k ON k.key_id=t.key
	WHERE k.key IN ('current_location_name','location_name')
		AND t.ts >= (extract(epoch FROM date_trunc('day', (SELECT now_ist FROM now_ctx))) * 1000)
),
loc_events_today_meaningful AS (
	SELECT
		entity_id,
		asset_label,
		location_label,
		ts,
		LAG(location_label) OVER (PARTITION BY entity_id ORDER BY ts) AS prev_location
	FROM loc_events_today
),
unknown_anchor_hotspots AS (
	SELECT
		location_label,
		COUNT(*)::bigint AS event_count,
		COUNT(DISTINCT asset_label)::bigint AS affected_assets
	FROM loc_events_today_meaningful
	WHERE (location_label ILIKE 'Unknown%' OR location_label='-NoDeviceName')
		AND (prev_location IS DISTINCT FROM location_label)
	GROUP BY location_label
	ORDER BY event_count DESC, location_label
	LIMIT 10
),
loc_events_7d AS (
	SELECT
		t.entity_id,
		COALESCE(NULLIF(trim(t.str_v),''), 'Unknown') AS location_label,
		t.ts,
		date_trunc('day', to_timestamp(t.ts/1000.0) AT TIME ZONE 'Asia/Kolkata')::date AS d
	FROM ts_kv t
	JOIN devices dev ON dev.id=t.entity_id
	JOIN key_dictionary k ON k.key_id=t.key
	WHERE k.key IN ('current_location_name','location_name')
		AND t.ts >= (extract(epoch FROM date_trunc('day', (SELECT now_ist FROM now_ctx) - interval '7 day')) * 1000)
),
loc_events_7d_meaningful AS (
	SELECT
		entity_id,
		location_label,
		d,
		LAG(location_label) OVER (PARTITION BY entity_id ORDER BY ts) AS prev_location
	FROM loc_events_7d
),
daily_unknown_7d AS (
	SELECT
		d,
		COUNT(*)::bigint AS unknown_events
	FROM loc_events_7d_meaningful
	WHERE d >= ((SELECT now_ist::date FROM now_ctx) - 6)
		AND (location_label ILIKE 'Unknown%' OR location_label='-NoDeviceName')
		AND (prev_location IS DISTINCT FROM location_label)
	GROUP BY 1
),
unknown_trend AS (
	SELECT
		COALESCE((SELECT SUM(event_count) FROM unknown_anchor_hotspots),0) AS unknown_events_today,
		COALESCE((SELECT AVG(unknown_events)::numeric FROM daily_unknown_7d),0) AS avg_unknown_events_7d
),
ap_assets AS (
	SELECT
		a.id,
		COALESCE(NULLIF(a.label,''), a.name) AS location_label,
		ap.name AS profile_name
	FROM asset a
	JOIN asset_profile ap ON ap.id = a.asset_profile_id
	WHERE ap.name IN ('AccessPointsGF','AccessPointGF','AccessPointsF1','AccessPointsNB')
),
location_metrics AS (
	SELECT
		COALESCE(NULLIF(aa.location_label,''), MAX(CASE WHEN k.key='location_name' THEN COALESCE(t.str_v,t.json_v::text) END), 'Unknown') AS location_label,
		aa.profile_name,
		COALESCE(MAX(CASE WHEN k.key='current_device_count' THEN COALESCE(t.long_v, NULLIF(regexp_replace(t.str_v, '[^0-9]', '', 'g'), '')::bigint) END), 0) AS assets_now,
		COALESCE(MAX(CASE WHEN k.key='peak_occupancy_today' THEN COALESCE(t.long_v, NULLIF(regexp_replace(t.str_v, '[^0-9]', '', 'g'), '')::bigint) END), 0) AS peak_occupancy,
		COALESCE(MAX(CASE WHEN k.key='daily_total_visits' THEN COALESCE(t.long_v, NULLIF(regexp_replace(t.str_v, '[^0-9]', '', 'g'), '')::bigint) END), 0) AS total_visits_today
	FROM ts_kv_latest t
	JOIN ap_assets aa ON aa.id=t.entity_id
	JOIN key_dictionary k ON k.key_id=t.key
	WHERE k.key IN ('location_name','current_device_count','peak_occupancy_today','daily_total_visits')
	GROUP BY t.entity_id, aa.location_label, aa.profile_name
),
location_pressure AS (
	SELECT
		location_label,
		profile_name,
		assets_now,
		peak_occupancy,
		total_visits_today,
		ROUND((0.5 * total_visits_today + 0.3 * assets_now + 0.2 * peak_occupancy)::numeric, 1) AS pressure_score
	FROM location_metrics
	WHERE location_label NOT ILIKE 'Unknown%'
),
top_stale AS (
	SELECT
		asset_label,
		current_location,
		ROUND(hours_since_seen::numeric, 1) AS hours_since_seen,
		last_seen_ist
	FROM stale_assets
	ORDER BY hours_since_seen DESC, asset_label
	LIMIT 15
),
unknown_recent AS (
	SELECT
		asset_label,
		current_location,
		ROUND(hours_since_seen::numeric, 1) AS hours_since_seen,
		last_seen_ist
	FROM unknown_assets
	ORDER BY last_seen_ts DESC NULLS LAST, asset_label
	LIMIT 20
),
risk_summary AS (
	SELECT
		(SELECT COUNT(*) FROM devices) AS total_assets,
		(SELECT COUNT(*) FROM unknown_assets) AS unknown_location_assets,
		(SELECT COUNT(*) FROM stale_assets WHERE hours_since_seen > 5) AS stale_gt_5h_assets,
		(SELECT COUNT(*) FROM stale_assets WHERE hours_since_seen > 24) AS stale_gt_24h_assets,
		(SELECT COUNT(*) FROM stale_assets WHERE hours_since_seen > 120) AS stale_gt_120h_assets,
		(SELECT unknown_events_today FROM unknown_trend) AS unknown_events_today,
		(SELECT avg_unknown_events_7d FROM unknown_trend) AS avg_unknown_events_7d
)
SELECT json_build_object(
	'generated_at_ist', to_char((SELECT now_ist FROM now_ctx), 'YYYY-MM-DD HH24:MI:SS') || ' IST',
	'risk_summary', (SELECT row_to_json(risk_summary) FROM risk_summary),
	'unknown_anchor_hotspots', (
		SELECT COALESCE(json_agg(
			json_build_object(
				'location_label', location_label,
				'event_count', event_count,
				'affected_assets', affected_assets
			) ORDER BY event_count DESC, location_label
		), '[]'::json)
		FROM unknown_anchor_hotspots
	),
	'unknown_assets_recent', (
		SELECT COALESCE(json_agg(
			json_build_object(
				'asset_label', asset_label,
				'current_location', current_location,
				'hours_since_seen', hours_since_seen,
				'last_seen_ist', last_seen_ist
			) ORDER BY hours_since_seen DESC, asset_label
		), '[]'::json)
		FROM unknown_recent
	),
	'stale_assets_recent', (
		SELECT COALESCE(json_agg(
			json_build_object(
				'asset_label', asset_label,
				'current_location', current_location,
				'hours_since_seen', hours_since_seen,
				'last_seen_ist', last_seen_ist
			) ORDER BY hours_since_seen DESC, asset_label
		), '[]'::json)
		FROM top_stale
	),
	'location_pressure', (
		SELECT COALESCE(json_agg(
			json_build_object(
				'location_label', location_label,
				'profile_name', profile_name,
				'assets_now', assets_now,
				'peak_occupancy', peak_occupancy,
				'total_visits_today', total_visits_today,
				'pressure_score', pressure_score
			) ORDER BY pressure_score DESC, location_label
		), '[]'::json)
		FROM location_pressure
	),
	'outcome_story', json_build_object(
		'visibility_coverage_pct', ROUND(((SELECT total_assets - unknown_location_assets FROM risk_summary)::numeric / GREATEST((SELECT total_assets FROM risk_summary),1) * 100), 1),
		'unknown_event_trend_vs_7d_pct', ROUND(
			CASE
				WHEN (SELECT avg_unknown_events_7d FROM risk_summary) <= 0 THEN 0
				ELSE (((SELECT unknown_events_today FROM risk_summary) - (SELECT avg_unknown_events_7d FROM risk_summary)) / (SELECT avg_unknown_events_7d FROM risk_summary)) * 100
			END
		, 1),
		'needs_anchor_mapping', ((SELECT unknown_location_assets FROM risk_summary) > 0)
	)
) AS payload;
EOF

if [[ "$RUN_LOCAL_DOCKER" == "true" ]]; then
	JSON_OUT=$(printf "%s\n" "$SQL" | docker exec -i "$TB_CONTAINER" psql -U thingsboard -d thingsboard -t -A | tail -n 1)

	BRIDGE_LOG=$(docker logs --since 24h "$BRIDGE_CONTAINER" 2>&1 || true)
	TB_LOG=$(docker logs --since 24h "$TB_CONTAINER" 2>&1 || true)
	BRIDGE_WS_RAW=$(printf "%s" "$BRIDGE_LOG" | grep 'Raw payload received' || true)

	BRIDGE_MISSING=$(printf "%s" "$BRIDGE_LOG" | grep -Eci 'No asset found for anchor|No asset config found for anchor' || true)
	BRIDGE_FENCE=$(printf "%s" "$BRIDGE_LOG" | grep -Eci 'Violation detected' || true)
	BRIDGE_TIMEOUT=$(printf "%s" "$BRIDGE_LOG" | grep -Eci 'ALERT_TIMEOUT|alert_name.:.Timeout|Timeout' || true)
	TB_WS_TIMEOUT=$(printf "%s" "$TB_LOG" | grep -Eci 'ping timeout' || true)
	TB_TRANSPORT_ERRORS=$(printf "%s" "$TB_LOG" | grep -Eci 'Message decoding failed|unknown message type' || true)

	WS_RAW_TMP=$(mktemp)
	printf "%s\n" "$BRIDGE_WS_RAW" > "$WS_RAW_TMP"
	WS_METRICS=$(python3 - "$WS_RAW_TMP" <<'PY'
import re
import sys

if len(sys.argv) < 2:
	print("WS_RAW_TOTAL_24H=0")
	print("WS_TELE_NONE_IGNORED_24H=0")
	print("WS_TRAC_TOTAL_24H=0")
	print("WS_TRAC_REF_CHANGE_24H=0")
	print("WS_STATUS_OFFLINE_TRANSITIONS_24H=0")
	print("WS_TIMEOUT_ALERTS_24H=0")
	print("WS_MEANINGFUL_EVENTS_24H=0")
	print("WS_IGNORED_EVENTS_24H=0")
	print("WS_IGNORED_RATIO_PCT_24H=0")
	raise SystemExit(0)

with open(sys.argv[1], 'r', encoding='utf-8', errors='ignore') as fh:
	lines = [ln for ln in fh.read().splitlines() if ln.strip()]

evt_re = re.compile(r"'evt_type':\s*'([^']+)'")
dev_re = re.compile(r"'device_id':\s*'([^']+)'")
event_re = re.compile(r"'event':\s*'([^']+)'")
ref_re = re.compile(r"'ref':\s*'([^']+)'")
status_re = re.compile(r"'status':\s*'([^']+)'")
alert_re = re.compile(r"'alert_id':\s*'([^']+)'")

raw_total = len(lines)
tele_none_ignored = 0
trac_total = 0
trac_ref_change = 0
status_offline_transition = 0
timeout_alerts = 0

last_ref_by_device = {}
last_status_by_device = {}

for ln in lines:
	evt_m = evt_re.search(ln)
	if not evt_m:
		continue

	evt = evt_m.group(1)
	dev_m = dev_re.search(ln)
	dev = dev_m.group(1) if dev_m else ""

	if evt == 'tele':
		event_m = event_re.search(ln)
		if event_m and event_m.group(1) == 'none':
			tele_none_ignored += 1

	elif evt == 'trac':
		trac_total += 1
		ref_m = ref_re.search(ln)
		ref = ref_m.group(1) if ref_m else ""
		if dev and ref:
			prev = last_ref_by_device.get(dev)
			if prev != ref:
				trac_ref_change += 1
				last_ref_by_device[dev] = ref

	elif evt == 'status':
		status_m = status_re.search(ln)
		status = status_m.group(1) if status_m else ""
		if dev and status:
			prev = last_status_by_device.get(dev)
			if status == 'offline' and prev != 'offline':
				status_offline_transition += 1
			last_status_by_device[dev] = status

	elif evt == 'alert':
		alert_m = alert_re.search(ln)
		if alert_m and alert_m.group(1) == 'ALERT_TIMEOUT':
			timeout_alerts += 1

meaningful_total = trac_ref_change + status_offline_transition + timeout_alerts
ignored_total = max(0, raw_total - meaningful_total)
ignored_ratio_pct = round((ignored_total / raw_total * 100.0), 1) if raw_total else 0.0

print(f"WS_RAW_TOTAL_24H={raw_total}")
print(f"WS_TELE_NONE_IGNORED_24H={tele_none_ignored}")
print(f"WS_TRAC_TOTAL_24H={trac_total}")
print(f"WS_TRAC_REF_CHANGE_24H={trac_ref_change}")
print(f"WS_STATUS_OFFLINE_TRANSITIONS_24H={status_offline_transition}")
print(f"WS_TIMEOUT_ALERTS_24H={timeout_alerts}")
print(f"WS_MEANINGFUL_EVENTS_24H={meaningful_total}")
print(f"WS_IGNORED_EVENTS_24H={ignored_total}")
print(f"WS_IGNORED_RATIO_PCT_24H={ignored_ratio_pct}")
PY
)
	rm -f "$WS_RAW_TMP"

	while IFS='=' read -r key value; do
		case "$key" in
			WS_RAW_TOTAL_24H|WS_TELE_NONE_IGNORED_24H|WS_TRAC_TOTAL_24H|WS_TRAC_REF_CHANGE_24H|WS_STATUS_OFFLINE_TRANSITIONS_24H|WS_TIMEOUT_ALERTS_24H|WS_MEANINGFUL_EVENTS_24H|WS_IGNORED_EVENTS_24H|WS_IGNORED_RATIO_PCT_24H)
				printf -v "$key" '%s' "$value"
				;;
		esac
	done <<< "$WS_METRICS"
else
	JSON_OUT=$(printf "%s\n" "$SQL" | sshpass -p "$TB_PASS" ssh -o StrictHostKeyChecking=no "${TB_USER}@${TB_HOST}" \
		"su - iotuser -c 'cd ${TB_BASE_PATH} && docker exec -i ${TB_CONTAINER} psql -U thingsboard -d thingsboard -t -A'" | tail -n 1)

	BRIDGE_MISSING=0
	BRIDGE_FENCE=0
	BRIDGE_TIMEOUT=0
	TB_WS_TIMEOUT=0
	TB_TRANSPORT_ERRORS=0
	WS_RAW_TOTAL_24H=0
	WS_TELE_NONE_IGNORED_24H=0
	WS_TRAC_TOTAL_24H=0
	WS_TRAC_REF_CHANGE_24H=0
	WS_STATUS_OFFLINE_TRANSITIONS_24H=0
	WS_TIMEOUT_ALERTS_24H=0
	WS_MEANINGFUL_EVENTS_24H=0
	WS_IGNORED_EVENTS_24H=0
	WS_IGNORED_RATIO_PCT_24H=0
fi

JSON_OUT="$JSON_OUT" \
BRIDGE_MISSING="$BRIDGE_MISSING" \
BRIDGE_FENCE="$BRIDGE_FENCE" \
BRIDGE_TIMEOUT="$BRIDGE_TIMEOUT" \
TB_WS_TIMEOUT="$TB_WS_TIMEOUT" \
TB_TRANSPORT_ERRORS="$TB_TRANSPORT_ERRORS" \
WS_RAW_TOTAL_24H="$WS_RAW_TOTAL_24H" \
WS_TELE_NONE_IGNORED_24H="$WS_TELE_NONE_IGNORED_24H" \
WS_TRAC_TOTAL_24H="$WS_TRAC_TOTAL_24H" \
WS_TRAC_REF_CHANGE_24H="$WS_TRAC_REF_CHANGE_24H" \
WS_STATUS_OFFLINE_TRANSITIONS_24H="$WS_STATUS_OFFLINE_TRANSITIONS_24H" \
WS_TIMEOUT_ALERTS_24H="$WS_TIMEOUT_ALERTS_24H" \
WS_MEANINGFUL_EVENTS_24H="$WS_MEANINGFUL_EVENTS_24H" \
WS_IGNORED_EVENTS_24H="$WS_IGNORED_EVENTS_24H" \
WS_IGNORED_RATIO_PCT_24H="$WS_IGNORED_RATIO_PCT_24H" \
python3 - <<'PY' > "$OUT_FILE"
import json
import os

payload = json.loads(os.environ.get("JSON_OUT") or "{}")
payload["reliability_signals"] = {
		"bridge_missing_anchor_warnings_24h": int(float(os.environ.get("BRIDGE_MISSING", "0"))),
		"bridge_fence_violations_24h": int(float(os.environ.get("BRIDGE_FENCE", "0"))),
		"bridge_timeout_alerts_24h": int(float(os.environ.get("BRIDGE_TIMEOUT", "0"))),
		"tb_websocket_ping_timeouts_24h": int(float(os.environ.get("TB_WS_TIMEOUT", "0"))),
		"tb_transport_decode_errors_24h": int(float(os.environ.get("TB_TRANSPORT_ERRORS", "0")))
}
payload["ws_signal_quality_24h"] = {
		"raw_ws_events_24h": int(float(os.environ.get("WS_RAW_TOTAL_24H", "0"))),
		"ignored_ws_events_24h": int(float(os.environ.get("WS_IGNORED_EVENTS_24H", "0"))),
		"ignored_ratio_pct_24h": float(os.environ.get("WS_IGNORED_RATIO_PCT_24H", "0")),
		"meaningful_ws_events_24h": int(float(os.environ.get("WS_MEANINGFUL_EVENTS_24H", "0"))),
		"tele_none_ignored_24h": int(float(os.environ.get("WS_TELE_NONE_IGNORED_24H", "0"))),
		"trac_total_24h": int(float(os.environ.get("WS_TRAC_TOTAL_24H", "0"))),
		"trac_ref_change_events_24h": int(float(os.environ.get("WS_TRAC_REF_CHANGE_24H", "0"))),
		"status_offline_transitions_24h": int(float(os.environ.get("WS_STATUS_OFFLINE_TRANSITIONS_24H", "0"))),
		"timeout_alerts_24h": int(float(os.environ.get("WS_TIMEOUT_ALERTS_24H", "0")))
}

print(json.dumps(payload, ensure_ascii=False))
PY

echo "Saved live v2 JSON to: $OUT_FILE"

