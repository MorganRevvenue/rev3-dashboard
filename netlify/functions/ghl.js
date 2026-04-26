const GHL_API_KEY = process.env.GHL_API_KEY;
const LOCATION_ID = process.env.GHL_LOCATION_ID;
const BASE = 'https://services.leadconnectorhq.com';

const headers = {
  'Authorization': `Bearer ${GHL_API_KEY}`,
  'Content-Type': 'application/json',
  'Version': '2021-07-28'
};

async function fetchAllOpportunities() {
  let all = [];
  let page = 1;
  let hasMore = true;

  while (hasMore) {
    const res = await fetch(
      `${BASE}/opportunities/search?location_id=${LOCATION_ID}&limit=100&page=${page}`,
      { headers }
    );
    const data = await res.json();
    const opps = data.opportunities || [];
    all = all.concat(opps);
    hasMore = opps.length === 100;
    page++;
  }
  return all;
}

async function fetchAllContacts() {
  let all = [];
  let page = 1;
  let hasMore = true;

  while (hasMore) {
    const res = await fetch(
      `${BASE}/contacts/?locationId=${LOCATION_ID}&limit=100&page=${page}`,
      { headers }
    );
    const data = await res.json();
    const contacts = data.contacts || [];
    all = all.concat(contacts);
    hasMore = contacts.length === 100;
    page++;
  }
  return all;
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
    const [opportunities, contacts] = await Promise.all([
      fetchAllOpportunities(),
      fetchAllContacts()
    ]);

    // Build contact map for quick lookup
    const contactMap = {};
    contacts.forEach(c => {
      contactMap[c.id] = c;
    });

    // Stage name mapping to dashboard categories
    const STAGE_MAP = {
      'Discovery Call Booked': 'dc_booked',
      'DC - No Show': 'dc_noshow',
      'DC - Cancelled': 'dc_cancelled',
      'Strategy Call Booked': 'sc_booked',
      'SC - No Show': 'sc_noshow',
      'SC - Cancelled': 'sc_cancelled',
      'FU SQL': 'fu_sql',
      'Closed Sale': 'closed',
      'Lost Sale': 'lost',
      'DQ/NQL': 'dq'
    };

    const LOOM_TAG = 'activity - instantly campaign - complete';

    // Process each opportunity
    const processed = opportunities.map(opp => {
      const contact = contactMap[opp.contactId] || {};
      const tags = contact.tags || [];
      const utmSource = contact.attributionSource?.utmSource || 
                        contact.customFields?.find(f => f.name === 'utm_source')?.value || '';

      const isLoom = tags.some(t => t.toLowerCase() === LOOM_TAG.toLowerCase());
      const isMeta = !isLoom && utmSource.toLowerCase().includes('facebook');
      const source = isLoom ? 'loom' : isMeta ? 'meta' : 'other';

      const stageName = opp.status === 'won' ? 'Closed Sale' : 
                        opp.status === 'lost' ? 'Lost Sale' :
                        opp.pipelineStage?.name || '';
      const stageKey = STAGE_MAP[stageName] || 'other';

      // Cohort week — use contact creation date
      const createdAt = new Date(opp.createdAt || contact.dateAdded || Date.now());
      const weekStart = getWeekStart(createdAt);

      return {
        id: opp.id,
        name: opp.name || `${contact.firstName || ''} ${contact.lastName || ''}`.trim(),
        source,
        stage: stageName,
        stageKey,
        weekStart,
        createdAt: createdAt.toISOString(),
        value: opp.monetaryValue || 0
      };
    });

    // Aggregate stats
    const stats = {
      total: processed.length,
      by_stage: {},
      by_source: { loom: {}, meta: {}, other: {} },
      by_week: {},
      pipeline_value: {
        active: 0,
        closed: 0,
        lost: 0
      }
    };

    // Count by stage
    Object.values(STAGE_MAP).forEach(key => {
      stats.by_stage[key] = 0;
    });

    processed.forEach(opp => {
      // By stage
      if (stats.by_stage[opp.stageKey] !== undefined) {
        stats.by_stage[opp.stageKey]++;
      }

      // By source + stage
      if (!stats.by_source[opp.source]) stats.by_source[opp.source] = {};
      if (!stats.by_source[opp.source][opp.stageKey]) stats.by_source[opp.source][opp.stageKey] = 0;
      stats.by_source[opp.source][opp.stageKey]++;

      // By week cohort
      if (!stats.by_week[opp.weekStart]) {
        stats.by_week[opp.weekStart] = {
          total: 0, loom: 0, meta: 0, other: 0,
          dc_booked: 0, dc_noshow: 0, dc_cancelled: 0,
          sc_booked: 0, sc_noshow: 0, sc_cancelled: 0,
          fu_sql: 0, closed: 0, lost: 0, dq: 0
        };
      }
      stats.by_week[opp.weekStart].total++;
      stats.by_week[opp.weekStart][opp.source]++;
      if (stats.by_week[opp.weekStart][opp.stageKey] !== undefined) {
        stats.by_week[opp.weekStart][opp.stageKey]++;
      }

      // Pipeline value
      if (opp.stageKey === 'closed') stats.pipeline_value.closed += opp.value;
      else if (opp.stageKey === 'lost' || opp.stageKey === 'dq') stats.pipeline_value.lost += opp.value;
      else stats.pipeline_value.active += opp.value;
    });

    // Sort weeks
    const sortedWeeks = Object.keys(stats.by_week).sort((a, b) => new Date(b) - new Date(a));
    stats.recent_weeks = sortedWeeks.slice(0, 6).map(w => ({
      week: w,
      ...stats.by_week[w]
    }));

    return {
      statusCode: 200,
      headers: corsHeaders,
      body: JSON.stringify({
        ok: true,
        stats,
        lastUpdated: new Date().toISOString(),
        totalOpportunities: processed.length
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

function getWeekStart(date) {
  const d = new Date(date);
  const day = d.getDay();
  const diff = d.getDate() - day + (day === 0 ? -6 : 1); // Monday start
  d.setDate(diff);
  d.setHours(0, 0, 0, 0);
  return d.toISOString().split('T')[0];
}
