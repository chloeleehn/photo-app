/*
 * LF2 - search-photos
 * Input: free text query ("cats and dogs")
 * Uses Lex to extract keywords, queries OpenSearch index "photos".
 */

const { Client } = require("@opensearch-project/opensearch");
const { LexRuntimeV2Client, RecognizeTextCommand } = require("@aws-sdk/client-lex-runtime-v2");

// Lex client
const lex = new LexRuntimeV2Client({});

// OpenSearch client with BASIC AUTH
const osClient = new Client({
  node: process.env.OPENSEARCH_ENDPOINT, 
  auth: {
    username: process.env.OPENSEARCH_USERNAME,
    password: process.env.OPENSEARCH_PASSWORD
  }
});

// Parse query with Amazon Lex (extract keywords)
async function extractKeywords(query) {
  try {
    const resp = await lex.send(
      new RecognizeTextCommand({
        botId: process.env.LEX_BOT_ID,
        botAliasId: process.env.LEX_BOT_ALIAS_ID,
        localeId: process.env.LEX_LOCALE || "en_US",
        sessionId: `sess-${Date.now()}`,
        text: query,
      })
    );

    console.log("Lex response:", JSON.stringify(resp));

    const slots = resp.sessionState?.intent?.slots || {};
    const keywords = [];

    // Extract keywords from all slots
    for (const slot of Object.values(slots)) {
      const val = slot?.value?.interpretedValue;
      if (!val) continue;

      const parts = val
        .toLowerCase()
        .split(/,|\band\b/) // split by commas or 'and'
        .map(s => s.trim())
        .filter(Boolean);

      keywords.push(...parts);
    }

    // Fallback tokenizer if Lex returned nothing
    if (keywords.length === 0) {
      console.log("Using fallback tokenizer");
      return query
        .toLowerCase()
        .replace(/\band\b/g, " ")
        .split(/\W+/)
        .filter(w => w.length > 2);
    }

    return Array.from(new Set(keywords)); // deduplicate
  } catch (err) {
    console.error("Lex failed:", err);
    // fallback if Lex fails
    return query
      .toLowerCase()
      .replace(/\band\b/g, " ")
      .split(/\W+/)
      .filter(w => w.length > 2);
  }
}

// Lambda Handler
exports.handler = async (event) => {
  console.log("Incoming event:", JSON.stringify(event));

  const query =
    event?.queryStringParameters?.q ||
    event?.q ||
    "";

  if (!query) {
    return {
      statusCode: 200,
      headers: { "Access-Control-Allow-Origin": "*" },
      body: JSON.stringify({ results: [] }),
    };
  }

  const keywords = await extractKeywords(query);
  console.log("Extracted keywords:", keywords);

  if (!keywords.length) {
    return {
      statusCode: 200,
      headers: { "Access-Control-Allow-Origin": "*" },
      body: JSON.stringify({ results: [] }),
    };
  }

  // Build multi-match query in OpenSearch
  const shouldClauses = keywords.map(k => ({
    match: { labels: { query: k, operator: "or" } }
  }));

  let searchResp;
  try {
    searchResp = await osClient.search({
      index: "photos",
      body: {
        query: {
          bool: { should: shouldClauses, minimum_should_match: 1 }
        }
      },
      size: 100
    });
  } catch (err) {
    console.error("OpenSearch query failed:", err);
    return {
      statusCode: 500,
      headers: { "Access-Control-Allow-Origin": "*" },
      body: JSON.stringify({ error: "Search failed" }),
    };
  }

  const hits = searchResp.body?.hits?.hits?.map(h => h._source) || [];

  return {
    statusCode: 200,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*"
    },
    body: JSON.stringify({ results: hits })
  };
};
