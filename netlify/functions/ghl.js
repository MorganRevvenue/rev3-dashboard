const GHL_API_KEY = process.env.GHL_API_KEY;
const LOCATION_ID = process.env.GHL_LOCATION_ID;
const BASE = 'https://services.leadconnectorhq.com';

const headers = {
  'Authorization': `Bearer ${GHL_API_KEY}`,
  'Content-Type': 'application/json',
  'Version': '2021-07-28'
};

// VSL Pipeline stage IDs mapped to dashboard keys
const STAGE_ID_MAP = {
  '4b850f35-eca8-4e6d-89f4-a2266ffa46f3': 'dc_booked',    // Discovery Call Booked
  '272f0b1e-a1bb-4399-9ea5-4e3bd86f372c': 'dc_noshow',    // DC - No Show
  'd01d76a9-a554-4768-8eb6-63c272451be0': 'dc_cancelled', // DC - Cancelled
  'd0c088bc-0f95-41ce-87dc-ed6bf767c8ae': 'sc_booked',    // Strategy Call Booked
  'c3895ca5-d69a-4260-872c-4a5801965c11': 'sc_noshow',    // SC - No Show
  '9f62f019-43b6-4052-84dd-e8abb497e16e': 'sc_cancelled', // SC - Cancelled
  'a0dc9a1c-e739-49bc-9e17-5af3f04db4e8': 'fu_sql',       // FU SQL
  '00ebb025-e7cd-4817-94cb-d5025651a282': 'closed',       // Closed Sale
  '398cf69d-dd87-4348-a37c-984aaa57d3f2': 'lost',         // Lost Sale
  '3abe2905-4446-43b7-9c55-9509b842f11a': 'dq'            // DQ/NQL
};

const VSL_STAGE_IDS = new Set(Object.keys(STAGE_ID_MAP));

const LOOM_TAG = 'activity - instantly campaign - complete';

async function fetchVSLOpportunities() {
  let all = [];
  let startAfterId = null;
  let hasMore = true;

  while (hasMore) {
    let url = `${BASE}/opportunities/search?location_id=${LOCATION_ID}&limit=100`;
    if (startAfterId) url += `&startAfterId=${startAfterId}`;

    const res = await fetch(url, { headers });
    const data = await res.json();
    const opps = (data.opportunities || []).filter(o => VSL_STAGE_IDS.has(o.pipelineStageId));
    all = all.concat(opps);

    const total = data.opportunities || [];
    if (total.length < 100) {
      hasMore = false;
    } else {
      startAfterId = total[total.length - 1].id;
      if (all.length > 3000) hasMore = false;
    }
  }
  return all;
}

async function fetchContact(contactId) {
  try {
    const res = await fetch(`${BASE}/contacts/${contactId}`, { headers });
    const data = await res.json();
    return data.contact || data || {};
  } catch (e) {
    return {};
  }
}

function getWeekStart(date) {
  const d = new Date(date);
  const day = d.getDay();
  const diff = d.getDate() - day + (day === 0 ? -6 : 1);
  d.setDate(diff);
  d.setHours(0, 0, 0, 0);
  return d.toISOString().split('T')[0];
}

exports.handler = async (event) => {
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: corsHeaders, body: '' };
  }

  try {
    const opportunities = await fetchVSLOpportunities();

    // Fetch contacts in batches
    const contactIds = [...new Set(opportunities.map(o => o.contactId).filter(Boolean))];
    const contactMap = {};
    
    // Fetch contacts in parallel batches of 20
    const batchSize = 20;
    for (let i = 0; i < contactIds.length; i += batchSize) {
      const batch = contactIds.slice(i, i + batchSize);
      const results = await Promise.all(batch.map(id => fetchContact(id)));
      results.forEach((contact, idx) => {
        if (contact) contactMap[batch[idx]] = contact;
      });
    }

    const stageKeys = ['dc_booked','dc_noshow','dc_cancelled','sc_booked','sc_noshow','sc_cancelled','fu_sql','closed','lost','dq'];
    
    const emptyStage = () => Object.fromEntries(stageKeys.map(k => [k, 0]));

    const stats = {
      total: 0,
      by_stage: emptyStage(),
      by_source: {
        loom: emptyStage(),
        meta: emptyStage(),
        other: emptyStage()
      },
      by_week: {},
      pipeline_value: { active: 0, closed: 0, lost: 0 }
    };

    opportunities.forEach(opp => {
      const stageKey = STAGE_ID_MAP[opp.pipelineStageId];
      if (!stageKey) return;

      const contact = contactMap[opp.contactId] || {};
      const tags = contact.tags || [];
      const utmSource = contact.attributionSource?.utmSource ||
        (contact.customFields || []).find(f =>
          f.fieldKey?.toLowerCase().includes('utm_source') ||
          f.name?.toLowerCase().includes('utm_source')
        )?.value || '';

      const isLoom = tags.some(t => t.toLowerCase().trim() === LOOM_TAG.toLowerCase().trim());
      const isMeta = !isLoom && utmSource.toLowerCase().includes('facebook');
      const source = isLoom ? 'loom' : isMeta ? 'meta' : 'other';

      const createdAt = new Date(opp.createdAt || Date.now());
      const weekStart = getWeekStart(createdAt);

      stats.total++;
      stats.by_stage[stageKey]++;
      stats.by_source[source][stageKey]++;

      if (!stats.by_week[weekStart]) {
        stats.by_week[weekStart] = { ...emptyStage(), total: 0, loom: 0, meta: 0, other: 0 };
      }
      stats.by_week[weekStart].total++;
      stats.by_week[weekStart][source]++;
      stats.by_week[weekStart][stageKey]++;

      const val = opp.monetaryValue || 0;
      if (stageKey === 'closed') stats.pipeline_value.closed += val;
      else if (stageKey === 'lost' || stageKey === 'dq') stats.pipeline_value.lost += val;
      else stats.pipeline_value.active += val;
    });

    const sortedWeeks = Object.keys(stats.by_week).sort((a, b) => new Date(b) - new Date(a));
    stats.recent_weeks = sortedWeeks.slice(0, 8).map(w => ({ week: w, ...stats.by_week[w] }));

    return {
      statusCode: 200,
      headers: corsHeaders,
      body: JSON.stringify({
        ok: true,
        stats,
        lastUpdated: new Date().toISOString(),
        totalOpportunities: stats.total
      })
    };

  } catch (err) {
    return {
      statusCode: 500,
      headers: corsHeaders,
      body: JSON.stringify({ ok: false, error: err.message, stack: err.stack })
    };
  }
};
