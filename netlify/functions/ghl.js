const GHL_API_KEY = process.env.GHL_API_KEY;
const LOCATION_ID = process.env.GHL_LOCATION_ID;
const BASE = 'https://services.leadconnectorhq.com';

const headers = {
  'Authorization': `Bearer ${GHL_API_KEY}`,
  'Content-Type': 'application/json',
  'Version': '2021-07-28'
};

// VSL Pipeline stage IDs -> dashboard keys
const STAGE_ID_MAP = {
  '4b850f35-eca8-4e6d-89f4-a2266ffa46f3': 'dc_booked',
  '272f0b1e-a1bb-4399-9ea5-4e3bd86f372c': 'dc_noshow',
  'd01d76a9-a554-4768-8eb6-63c272451be0': 'dc_cancelled',
  'd0c088bc-0f95-41ce-87dc-ed6bf767c8ae': 'sc_booked',
  'c3895ca5-d69a-4260-872c-4a5801965c11': 'sc_noshow',
  '9f62f019-43b6-4052-84dd-e8abb497e16e': 'sc_cancelled',
  'a0dc9a1c-e739-49bc-9e17-5af3f04db4e8': 'fu_sql',
  '00ebb025-e7cd-4817-94cb-d5025651a282': 'closed',
  '398cf69d-dd87-4348-a37c-984aaa57d3f2': 'lost',
  '3abe2905-4446-43b7-9c55-9509b842f11a': 'dq'
};

const VSL_STAGE_IDS = new Set(Object.keys(STAGE_ID_MAP));
const LOOM_TAG = 'activity - instantly campaign - complete';

function getWeekStart(date) {
  const d = new Date(date);
  const day = d.getDay();
  const diff = d.getDate() - day + (day === 0 ? -6 : 1);
  d.setDate(diff);
  d.setHours(0, 0, 0, 0);
  return d.toISOString().split('T')[0];
}

function getCurrentWeekStart() { return getWeekStart(new Date()); }
function getLastWeekStart() {
  const d = new Date();
  d.setDate(d.getDate() - 7);
  return getWeekStart(d);
}

// Fetch each VSL stage separately so we ONLY get VSL pipeline opps
async function fetchStageOpportunities(stageId) {
  let all = [];
  let startAfterId = null;
  let hasMore = true;
  let pages = 0;

  while (hasMore && pages < 20) {
    let url = `${BASE}/opportunities/search?location_id=${LOCATION_ID}&limit=100&pipelineStageId=${stageId}`;
    if (startAfterId) url += `&startAfterId=${startAfterId}`;
    
    const res = await fetch(url, { headers });
    const data = await res.json();
    const opps = data.opportunities || [];
    all = all.concat(opps);
    
    if (opps.length < 100) { hasMore = false; }
    else { startAfterId = opps[opps.length - 1].id; }
    pages++;
  }
  return all;
}

async function fetchContactInfo(contactId) {
  try {
    const res = await fetch(`${BASE}/contacts/${contactId}`, { headers });
    const data = await res.json();
    const c = data.contact || data || {};
    return {
      tags: c.tags || [],
      utmSource: c.attributionSource?.utmSource ||
        (c.customFields || []).find(f =>
          f.fieldKey?.toLowerCase().includes('utm_source'))?.value || ''
    };
  } catch(e) { return { tags: [], utmSource: '' }; }
}

exports.handler = async (event) => {
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json'
  };
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: corsHeaders, body: '' };

  try {
    // Fetch all VSL stages in parallel
    const stageIds = Object.keys(STAGE_ID_MAP);
    const stageResults = await Promise.all(stageIds.map(id => fetchStageOpportunities(id)));
    
    // Flatten and deduplicate
    const allOpps = [];
    const seen = new Set();
    stageResults.forEach((opps, idx) => {
      opps.forEach(opp => {
        if (!seen.has(opp.id)) {
          seen.add(opp.id);
          allOpps.push({ ...opp, _stageKey: STAGE_ID_MAP[stageIds[idx]] });
        }
      });
    });

    // Fetch contact info in batches of 15
    const contactIds = [...new Set(allOpps.map(o => o.contactId).filter(Boolean))];
    const contactData = {};
    const batchSize = 15;
    
    for (let i = 0; i < contactIds.length; i += batchSize) {
      const batch = contactIds.slice(i, i + batchSize);
      const results = await Promise.all(batch.map(id => fetchContactInfo(id)));
      batch.forEach((id, idx) => { contactData[id] = results[idx]; });
    }

    const stageKeys = ['dc_booked','dc_noshow','dc_cancelled','sc_booked','sc_noshow','sc_cancelled','fu_sql','closed','lost','dq'];
    const emptyStage = () => Object.fromEntries(stageKeys.map(k => [k, 0]));
    const emptyWeek = () => ({ ...emptyStage(), total: 0, loom: 0, meta: 0, other: 0 });

    const thisWeek = getCurrentWeekStart();
    const lastWeek = getLastWeekStart();

    const stats = {
      total: 0,
      by_stage: emptyStage(),
      by_source: { loom: emptyStage(), meta: emptyStage(), other: emptyStage() },
      this_week: emptyWeek(),
      last_week: emptyWeek(),
      by_week: {},
      pipeline_value: { active: 0, closed: 0, lost: 0 }
    };

    allOpps.forEach(opp => {
      const stageKey = opp._stageKey;
      if (!stageKey) return;

      const cd = contactData[opp.contactId] || { tags: [], utmSource: '' };
      const isLoom = cd.tags.some(t => t.toLowerCase().trim() === LOOM_TAG);
      const isMeta = !isLoom && cd.utmSource.toLowerCase().includes('facebook');
      const source = isLoom ? 'loom' : isMeta ? 'meta' : 'other';

      const weekStart = getWeekStart(opp.createdAt || Date.now());

      stats.total++;
      stats.by_stage[stageKey]++;
      stats.by_source[source][stageKey]++;

      if (weekStart === thisWeek) {
        stats.this_week.total++;
        stats.this_week[source]++;
        stats.this_week[stageKey]++;
      }
      if (weekStart === lastWeek) {
        stats.last_week.total++;
        stats.last_week[source]++;
        stats.last_week[stageKey]++;
      }

      if (!stats.by_week[weekStart]) stats.by_week[weekStart] = emptyWeek();
      stats.by_week[weekStart].total++;
      stats.by_week[weekStart][source]++;
      stats.by_week[weekStart][stageKey]++;

      const val = opp.monetaryValue || 0;
      if (stageKey === 'closed') stats.pipeline_value.closed += val;
      else if (stageKey === 'lost' || stageKey === 'dq') stats.pipeline_value.lost += val;
      else stats.pipeline_value.active += val;
    });

    const sortedWeeks = Object.keys(stats.by_week).sort((a, b) => new Date(b) - new Date(a));
    stats.recent_weeks = sortedWeeks.slice(0, 10).map(w => ({ week: w, ...stats.by_week[w] }));

    return {
      statusCode: 200,
      headers: corsHeaders,
      body: JSON.stringify({
        ok: true, stats, thisWeek, lastWeek,
        lastUpdated: new Date().toISOString(),
        totalOpportunities: stats.total
      })
    };

  } catch (err) {
    return {
      statusCode: 500,
      headers: corsHeaders,
      body: JSON.stringify({ ok: false, error: err.message })
    };
  }
};
