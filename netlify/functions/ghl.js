const GHL_API_KEY = process.env.GHL_API_KEY;
const LOCATION_ID = process.env.GHL_LOCATION_ID;
const BASE = 'https://services.leadconnectorhq.com';

const headers = {
  'Authorization': `Bearer ${GHL_API_KEY}`,
  'Content-Type': 'application/json',
  'Version': '2021-07-28'
};

// Fetch all opportunities with pagination
async function fetchAllOpportunities() {
  let all = [];
  let startAfter = null;
  let hasMore = true;

  while (hasMore) {
    let url = `${BASE}/opportunities/search?location_id=${LOCATION_ID}&limit=100`;
    if (startAfter) url += `&startAfter=${startAfter}`;
    
    const res = await fetch(url, { headers });
    const data = await res.json();
    const opps = data.opportunities || [];
    all = all.concat(opps);
    
    // GHL uses cursor-based pagination
    if (opps.length < 100 || !data.meta?.nextPageUrl) {
      hasMore = false;
    } else {
      startAfter = opps[opps.length - 1].id;
      if (all.length > 5000) hasMore = false; // safety limit
    }
  }
  return all;
}

// Fetch pipelines to get stage IDs and names
async function fetchPipelines() {
  const res = await fetch(
    `${BASE}/opportunities/pipelines?locationId=${LOCATION_ID}`,
    { headers }
  );
  const data = await res.json();
  return data.pipelines || [];
}

// Fetch contacts with pagination
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
    if (page > 50) hasMore = false; // safety limit
  }
  return all;
}

// Map stage name to dashboard key using fuzzy matching
function mapStage(stageName) {
  if (!stageName) return 'other';
  const s = stageName.toLowerCase().trim();
  if (s.includes('discovery') && (s.includes('book') || s.includes('booked'))) return 'dc_booked';
  if ((s.includes('dc') || s.includes('discovery')) && s.includes('no') && s.includes('show')) return 'dc_noshow';
  if ((s.includes('dc') || s.includes('discovery')) && s.includes('cancel')) return 'dc_cancelled';
  if ((s.includes('strategy') || s.includes('sc')) && (s.includes('book') || s.includes('booked'))) return 'sc_booked';
  if ((s.includes('strategy') || s.includes('sc')) && s.includes('no') && s.includes('show')) return 'sc_noshow';
  if ((s.includes('strategy') || s.includes('sc')) && s.includes('cancel')) return 'sc_cancelled';
  if (s.includes('fu') || s.includes('follow') || s.includes('sql')) return 'fu_sql';
  if (s.includes('closed sale') || s.includes('close')) return 'closed';
  if (s.includes('lost')) return 'lost';
  if (s.includes('dq') || s.includes('nql') || s.includes('disqualif')) return 'dq';
  return 'other';
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
    // Fetch pipelines first to build stage ID -> name map
    const pipelines = await fetchPipelines();
    const stageMap = {}; // stageId -> stageName
    let debugStages = [];
    
    pipelines.forEach(pipeline => {
      (pipeline.stages || []).forEach(stage => {
        stageMap[stage.id] = stage.name;
        debugStages.push({ id: stage.id, name: stage.name, pipeline: pipeline.name });
      });
    });

    // Fetch opportunities and contacts in parallel
    const [opportunities, contacts] = await Promise.all([
      fetchAllOpportunities(),
      fetchAllContacts()
    ]);

    // Build contact lookup map
    const contactMap = {};
    contacts.forEach(c => { contactMap[c.id] = c; });

    const LOOM_TAG = 'activity - instantly campaign - complete';

    // Process each opportunity
    const processed = opportunities.map(opp => {
      const contact = contactMap[opp.contactId] || {};
      const tags = contact.tags || [];
      
      // Check UTM source for Meta leads
      const utmSource = contact.attributionSource?.utmSource || 
                        (contact.customFields || []).find(f => 
                          f.name?.toLowerCase().includes('utm_source') || 
                          f.fieldKey?.toLowerCase().includes('utm_source')
                        )?.value || '';

      const isLoom = tags.some(t => t.toLowerCase().trim() === LOOM_TAG.toLowerCase().trim());
      const isMeta = !isLoom && utmSource.toLowerCase().includes('facebook');
      const source = isLoom ? 'loom' : isMeta ? 'meta' : 'other';

      // Get stage name from stageMap using pipelineStageId
      const stageId = opp.pipelineStageId || opp.stageId || '';
      const stageName = stageMap[stageId] || opp.pipelineStage?.name || opp.status || '';
      
      let stageKey;
      if (opp.status === 'won') stageKey = 'closed';
      else if (opp.status === 'lost') stageKey = 'lost';
      else stageKey = mapStage(stageName);

      const createdAt = new Date(opp.createdAt || contact.dateAdded || Date.now());
      const weekStart = getWeekStart(createdAt);

      return {
        id: opp.id,
        source,
        stage: stageName,
        stageKey,
        weekStart,
        value: opp.monetaryValue || 0,
        status: opp.status
      };
    });

    // Aggregate stats
    const stageKeys = ['dc_booked','dc_noshow','dc_cancelled','sc_booked','sc_noshow','sc_cancelled','fu_sql','closed','lost','dq','other'];
    
    const stats = {
      total: processed.length,
      by_stage: Object.fromEntries(stageKeys.map(k => [k, 0])),
      by_source: { loom: Object.fromEntries(stageKeys.map(k => [k, 0])), meta: Object.fromEntries(stageKeys.map(k => [k, 0])), other: Object.fromEntries(stageKeys.map(k => [k, 0])) },
      by_week: {},
      pipeline_value: { active: 0, closed: 0, lost: 0 }
    };

    processed.forEach(opp => {
      stats.by_stage[opp.stageKey] = (stats.by_stage[opp.stageKey] || 0) + 1;
      
      if (!stats.by_source[opp.source]) stats.by_source[opp.source] = Object.fromEntries(stageKeys.map(k => [k, 0]));
      stats.by_source[opp.source][opp.stageKey] = (stats.by_source[opp.source][opp.stageKey] || 0) + 1;

      if (!stats.by_week[opp.weekStart]) {
        stats.by_week[opp.weekStart] = Object.fromEntries(stageKeys.map(k => [k, 0]));
        stats.by_week[opp.weekStart].total = 0;
        stats.by_week[opp.weekStart].loom = 0;
        stats.by_week[opp.weekStart].meta = 0;
        stats.by_week[opp.weekStart].other = 0;
      }
      stats.by_week[opp.weekStart].total++;
      stats.by_week[opp.weekStart][opp.source]++;
      stats.by_week[opp.weekStart][opp.stageKey] = (stats.by_week[opp.weekStart][opp.stageKey] || 0) + 1;

      if (opp.stageKey === 'closed') stats.pipeline_value.closed += opp.value;
      else if (opp.stageKey === 'lost' || opp.stageKey === 'dq') stats.pipeline_value.lost += opp.value;
      else stats.pipeline_value.active += opp.value;
    });

    const sortedWeeks = Object.keys(stats.by_week).sort((a, b) => new Date(b) - new Date(a));
    stats.recent_weeks = sortedWeeks.slice(0, 8).map(w => ({ week: w, ...stats.by_week[w] }));

    return {
      statusCode: 200,
      headers: corsHeaders,
      body: JSON.stringify({
        ok: true,
        stats,
        debugStages, // shows all stage names from GHL so we can verify mapping
        lastUpdated: new Date().toISOString(),
        totalOpportunities: processed.length
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
