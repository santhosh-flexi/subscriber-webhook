import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand, GetCommand, PutCommand } from "@aws-sdk/lib-dynamodb";
import https from "https";
import http from "http";
import crypto from "crypto";
import { URL } from "url";

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

const RULE_EVENT_LABELS = {
  1: "purchased",
  3: "cart_abandoned",
  5: "bump_purchased",
  9: "manual_member",
};

async function claimPurchaseDelivery(tabPrefix, subscriberId) {
  if (!subscriberId) return true;
  try {
    await docClient.send(
      new PutCommand({
        TableName: tabPrefix + "autoresponder_data",
        Item: {
          subscriber_id: `purchwh_claim_${subscriberId}`,
          autoresponder: "webhook_claim",
          created_at: new Date().toISOString().replace("T", " ").substring(0, 19),
        },
        ConditionExpression: "attribute_not_exists(subscriber_id)",
      })
    );
    return true;
  } catch (error) {
    if (error?.name === "ConditionalCheckFailedException") return false;
    console.error(`[webhook.claim_failed] ${error?.message} action=deliver_anyway`);
    return true;
  }
}

export const handler = async (event, context) => {
  console.log('Webhook Lambda triggered');
  console.log('Event:', JSON.stringify(event, null, 2).replace(/(renew=[^.&"\\]+\.)[A-Za-z0-9]+/g, "$1[redacted]"));

  const detail = event.detail;

  if (detail?.subscriptionEvent) {
    return handleSubscriptionEvent(detail, context);
  }

  const environment = detail.environment;
  const subscriberData = detail.subscriberData;
  // const tabPrefix = environment === "dev" ? "dev" : "";
  const tabPrefix = "";

  console.log(`Processing webhooks for subscriber: ${subscriberData.subscriber_id}`);

  if (detail.eventName && detail.eventName !== "INSERT") {
    console.log(`[webhook.skip] eventName=${detail.eventName} subscriber=${subscriberData.subscriber_id}`);
    return { statusCode: 200, message: "Non-insert stream event skipped" };
  }


  // Parse the data string into object
  let postData;
  try {
    postData = typeof subscriberData.data === "string"
      ? JSON.parse(subscriberData.data)
      : subscriberData.data;
  } catch (error) {
    console.error('Failed to parse subscriber data:', error);
    return { statusCode: 400, error: 'Invalid data format' };
  }

  // Determine cart abandonment skip
  const cartAbondSkip = subscriberData.cart_user_id ? 1 : 0;

  if (cartAbondSkip === 1) {
    console.log('Processing cart abandonment webhooks');
  }

  try {
    console.log("Starting webhook processing");

    // Determine webhook query parameters
    const { webhookParams, shouldProcess } = buildWebhookQuery(
      postData,
      subscriberData,
      tabPrefix
    );

    if (!shouldProcess) {
      console.log('No webhook configuration');
      return { statusCode: 200, message: 'No webhook configuration' };
    }

    // Query webhook rules
    const command = new QueryCommand(webhookParams);
    const whData = await docClient.send(command);
    console.log(`Found ${whData.Count} webhook rules`);

    // Prepare payment form data
    const paymentForm = preparePaymentForm(postData, subscriberData);
    if (cartAbondSkip === 0) {
      await addSubscriptionDataToForm(paymentForm, postData);
    }
    console.log('Payment form prepared:', { ...paymentForm, ...(paymentForm.pay_now_url ? { pay_now_url: '[redacted]' } : {}) });

    // Parse bump data
    const { rlBumpPid, bumpData } = parseBumpData(subscriberData);

    // Add bump data to payment form
    if (bumpData.length > 0) {
      addBumpDataToForm(paymentForm, bumpData);
    }

    // Check if webhook should proceed
    const shouldProceed = shouldWebhookProceed(
      whData,
      postData,
      subscriberData.product_id
    );

    if (!shouldProceed) {
      console.log('Webhook not proceeding - no valid configuration');
      return { statusCode: 200, message: 'Webhook not proceeding' };
    }

    // Claimed here rather than at entry: a failure before this point must stay
    // retryable, or a transient Dynamo error would permanently drop the delivery.
    const claimed = await claimPurchaseDelivery(tabPrefix, subscriberData.subscriber_id);
    if (!claimed) {
      console.log(`[webhook.duplicate] subscriber=${subscriberData.subscriber_id} skipped`);
      return { statusCode: 200, message: "Duplicate delivery skipped" };
    }

    // Process all webhooks with Promise.allSettled (no batch failures)
    const webhookPromises = whData.Items.map(async (wh) => {
      const shouldSend = shouldSendWebhook(
        wh,
        postData,
        cartAbondSkip,
        rlBumpPid
      );

      if (shouldSend) {
        // Add mobile if exists
        const finalPaymentForm = {
          ...paymentForm,
          event:
            wh.rule_type === "page"
              ? "optin"
              : cartAbondSkip === 1
                ? "cart_abandoned"
                : RULE_EVENT_LABELS[Number(wh.rule_id)] || "purchased",
        };
        if (subscriberData.mobile) {
          finalPaymentForm.mobile = subscriberData.mobile;
        }

        return await processWebhook(
          wh,
          subscriberData,
          finalPaymentForm,
          tabPrefix
        );
      } else {
        // ADD THIS:
        console.log(`[Webhook ${wh.product_rule_id}] SKIPPED - Rule ${wh.rule_id}, Type: ${wh.rule_type}, Pricing: ${wh.pricing_id}, IncomingPricing: ${JSON.stringify(postData.pricing_id)}, EventRule: ${Number(postData.rule_id) || 0}`);
        return { skipped: true, rule_id: wh.product_rule_id, reason: 'rule_mismatch' };
      }

      return { skipped: true, rule_id: wh.product_rule_id };
    });

    const results = await Promise.allSettled(webhookPromises);

    // Count successes and failures
    const processed = results.filter(r => r.status === 'fulfilled' && !r.value?.skipped).length;
    const failed = results.filter(r => r.status === 'rejected').length;
    const skipped = results.filter(r => r.status === 'fulfilled' && r.value?.skipped).length;

    console.log(`Webhook processing complete: ${processed} processed, ${failed} failed, ${skipped} skipped`);

    return {
      statusCode: 200,
      message: `Processed ${processed} webhooks, ${failed} failed, ${skipped} skipped`,
      details: { processed, failed, skipped }
    };

  } catch (error) {
    console.error('Error processing webhooks:', error);
    return {
      statusCode: 500,
      error: error.message
    };
  }
};

/**
 * Build webhook query parameters based on form_id or product_id
 */
function buildWebhookQuery(postData, subscriberData, tabPrefix) {
  let webhookParams;
  let shouldProcess = false;

  // Check for product_id first
  if (typeof postData.form_id === "undefined") {
    if (!subscriberData.product_id || subscriberData.product_id == 0) {
      return { webhookParams: null, shouldProcess: false };
    }

    webhookParams = {
      TableName: tabPrefix + "product_rules",
      IndexName: "product_id-index",
      KeyConditionExpression: "product_id = :v2",
      FilterExpression: "#type_of_flow = :v3",
      ExpressionAttributeValues: {
        ":v2": Number(subscriberData.product_id),
        ":v3": "webhook",
      },
      ExpressionAttributeNames: {
        "#type_of_flow": "autoresponder",
      },
    };
    shouldProcess = true;
  }
  // Check for form_id
  else if (typeof postData.form_id !== "undefined") {
    webhookParams = {
      TableName: tabPrefix + "product_rules",
      IndexName: "form_id-index",
      KeyConditionExpression: "form_id = :v2",
      FilterExpression: "#type_of_flow = :v3 AND funnel_page_id = :v4",
      ExpressionAttributeValues: {
        ":v2": postData.form_id,
        ":v3": "form",
        ":v4": subscriberData.funnel_page_id,
      },
      ExpressionAttributeNames: {
        "#type_of_flow": "autoresponder",
      },
    };
    shouldProcess = true;
  }

  return { webhookParams, shouldProcess };
}

/**
 * Prepare payment form data from subscriber data
 */
function preparePaymentForm(postData, subscriberData) {
  const paymentForm = { ...postData };

  if (postData.pay_currency) {
    paymentForm.currency = postData.pay_currency;
  }

  // Transform product data
  if (typeof paymentForm.p_id !== "undefined") {
    paymentForm.product_id = paymentForm.p_id;
    paymentForm.sales_page_url = paymentForm.url;
    if (paymentForm.price) {
      paymentForm.price = paymentForm.price.toString();
    }
  }

  // Parse and add UTM parameters
  if (paymentForm.flexi_tracking && paymentForm.flexi_tracking !== "") {
    try {
      const utmObj = JSON.parse(paymentForm.flexi_tracking);
      if (utmObj?.params) {
        const p = utmObj.params;
        // Never overwrite a good top-level value with undefined. The browser sends
        // utm_* at the top level AND a flexi_tracking.params object whose keys belong
        // to the ff scheme (source / utmmedium / medium / targetingname / adname).
        // Reading only params.utm_* blanked these five fields in every vendor webhook
        // the day those keys were renamed. Order of preference: legacy key, ff-scheme
        // equivalent, then whatever was already on the form.
        const firstSet = (...vals) =>
          vals.find((v) => v !== undefined && v !== null && v !== '');
        const keep = (field, ...vals) => {
          const v = firstSet(...vals);
          if (v !== undefined) paymentForm[field] = v;
        };
        keep('utm_source',   paymentForm.utm_source,   p.utm_source,   p.source);
        keep('utm_medium',   paymentForm.utm_medium,   p.utm_medium,   p.utmmedium);
        keep('utm_campaign', paymentForm.utm_campaign, p.utm_campaign, p.medium);
        keep('utm_term',     paymentForm.utm_term,     p.utm_term,     p.targetingname);
        keep('utm_content',  paymentForm.utm_content,  p.utm_content,  p.adname);
      }
    } catch (e) {
      console.log('Failed to parse flexi_tracking:', e.message);
    }
  }

  // Remove unwanted fields
  const fieldsToRemove = [
    'pricing_id', 'integration_id', 'paymentcommonradio', 'product_description',
    'rule_id', 'product_type', 'digital_asset', 'bump', 'paymentMethodType',
    'fid', 'ffid', 'querystring', 'funnel_id', 'form_id', 'funnel_page_id',
    'noaffi', 'device', 'erq_mode', '_fbc', 'uuid', '_fbp', 'utm_ffid',
    'url', 'flexi_tracking', 'site_id', 'p_id', 'ft_countrycode',
    'date_time', 'touch_point', 'pathd', 'fbclid', 'ffu_id', 'gclid', 'attempt_seed'
  ];

  fieldsToRemove.forEach(field => delete paymentForm[field]);

  // Add gclid only if it exists and is not empty
  if (postData.gclid && postData.gclid.trim() !== '') {
    paymentForm.gclid = postData.gclid;
  }

  // Add fbclid only if it exists and is not empty
  if (postData.fbclid && postData.fbclid.trim() !== '') {
    paymentForm.fbclid = postData.fbclid;
  }

  // Add datetime in IST
  const zonedatetime = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
  paymentForm.datetime = zonedatetime;

  return paymentForm;
}

const BILLING_INTERVAL_ALIASES = {
  quaterly: "quarterly",
  quartely: "quarterly",
  quarter: "quarterly",
  annual: "yearly",
  annually: "yearly",
  year: "yearly",
  month: "monthly",
  week: "weekly",
  day: "daily",
};

const BILLING_INTERVAL_PLURALS = {
  yearly: "years",
  quarterly: "quarters",
  monthly: "months",
  weekly: "weeks",
  daily: "days",
};

function billingFrequencyLabel(interval, intervalCount) {
  const raw = String(interval || "").trim().toLowerCase();
  if (!raw) {
    return null;
  }
  if (/^every_\d+_months$/.test(raw)) {
    return raw.replace(/_/g, " ");
  }
  const unit = BILLING_INTERVAL_ALIASES[raw] || raw;
  const count = Math.max(1, Number(intervalCount) || 1);

  if (unit === "monthly" && count === 3) return "quarterly";
  if (unit === "monthly" && count === 12) return "yearly";
  return count > 1
    ? `every ${count} ${BILLING_INTERVAL_PLURALS[unit] || unit}`
    : unit;
}

async function addSubscriptionDataToForm(paymentForm, postData) {
  const email = postData.email;
  const orderId = postData.order_id;
  if (!email || !orderId) {
    return;
  }

  let subscription;
  const renewalSubscriptionId = postData.renewal_subscription_id ? String(postData.renewal_subscription_id) : "";
  try {
    const result = await docClient.send(
      new QueryCommand({
        TableName: "customer_subscriptions",
        IndexName: "customer_email_index",
        KeyConditionExpression: "customer_email = :email",
        ExpressionAttributeValues: { ":email": email },
      })
    );
    const productId = Number(postData.p_id || postData.product_id) || 0;
    subscription = (result.Items || []).find(
      (item) =>
        (renewalSubscriptionId
          ? String(item?.subscription_id) === renewalSubscriptionId
          : String(item?.data?.order_id || "") === String(orderId)) &&
        (!productId || Number(item?.product_id) === productId)
    );
  } catch (error) {
    console.log(
      `[webhook.purchase.sub_lookup_failed] order=${orderId} error=${error?.message}`
    );
    return;
  }

  if (!subscription) {
    console.log(`[webhook.purchase.sub_missing] order=${orderId}`);
    return;
  }

  // A row created at checkout has status "pending" and no user_status yet, and
  // its period end stays "" until the gateway lifecycle webhook lands. Send only
  // what actually has a value rather than empty strings the vendor has to filter.
  const fields = {
    subscription_event: renewalSubscriptionId ? "subscription.renewal_payment" : "subscription.created",
    subscription_id: subscription.subscription_id,
    status: subscription.user_status || subscription.status,
    plan_name: subscription.data?.plan_name,
    ...vendorBillingInterval(
      subscription.billing_interval,
      subscription.billing_interval_count
    ),
    billing_frequency: billingFrequencyLabel(
      subscription.billing_interval,
      subscription.billing_interval_count
    ),
    next_billing_date: subscription.current_period_end,
    setup_fee: subscription.setup_fee,
    recurring_amount: subscription.amount,
    tax_mode: subscription.data?.tax_mode,
    tax_status: subscription.data?.tax_status === "dropped" ? "not_enabled" : subscription.data?.tax_status,
    billing_mode: subscription.data?.billing_mode,
    pay_now_url: subscription.data?.billing_mode === "manual" && !renewalSubscriptionId ? subscription.data?.renewal_url : undefined,
    grace_days: subscription.data?.billing_mode === "manual" ? subscription.data?.grace_days : undefined,
  };
  for (const [key, value] of Object.entries(fields)) {
    if (value !== undefined && value !== null && value !== "") {
      paymentForm[key] = value;
    }
  }

  console.log(
    `[webhook.purchase.sub_attached] order=${orderId} sub=${subscription.subscription_id} frequency=${paymentForm.billing_frequency}`
  );
}

/**
 * Parse bump data from subscriber data
 */
function parseBumpData(subscriberData) {
  let bumpData = [];
  let rlBumpPid = [];

  try {
    bumpData = typeof subscriberData.bump === "string"
      ? JSON.parse(subscriberData.bump)
      : subscriberData.bump;

    if (!Array.isArray(bumpData)) {
      bumpData = [];
    }
  } catch (error) {
    console.error('Failed to parse bump data:', error);
    bumpData = [];
  }

  // Extract bump pricing IDs
  if (bumpData.length > 0) {
    // rlBumpPid = bumpData.map(bm => bm.pricing_id).filter(Boolean);
    rlBumpPid = bumpData.map(bm => bm.b_pid).filter(Boolean);
  }

  return { rlBumpPid, bumpData };
}

/**
 * Add bump data to payment form
 */
function addBumpDataToForm(paymentForm, bumpData) {
  let sumPrice = 0;

  bumpData.forEach((bm, index) => {
    sumPrice += Number(bm.price) || 0;
    paymentForm[bm.name] = 1;
    paymentForm[`${bm.name}_price`] = bm.price;
  });

  paymentForm.total_bump_price = sumPrice;
}

/**
 * Check if webhook processing should proceed
 */
function shouldWebhookProceed(whData, postData, productId) {
  // Check if we have valid webhook rules
  if (whData.Count > 0 && whData.Items[0].webhook_url && whData.Items[0].webhook_url !== "null") {
    return true;
  }

  // Global webhook for specific site (legacy support)
  if (postData.site_id === "b68n9p.flexifunnels.com" && productId && productId != 0) {
    const globWebhook = {
      rule_id: 0,
      pricing_id: 0,
      webhook_name: "global-webhook",
      rule_type: "glob",
      webhook_url: "https://connect.pabbly.com/workflow/sendwebhookdata/IjU3NjYwNTZiMDYzZjA0MzE1MjZhNTUzMTUxMzYi_pc"
    };
    whData.Items.push(globWebhook);
    console.log("Global webhook added for b68n9p.flexifunnels.com");
    return true;
  }

  return false;
}

/**
 * Determine if webhook should be sent based on rules
 */
function shouldSendWebhook(wh, postData, cartAbondSkip, rlBumpPid) {
  // console.log(wh, postData, cartAbondSkip, rlBumpPid);
  // The event that actually happened, not the rule being tested. A manually
  // added member arrives as rule 9 and must not satisfy the purchase branch.
  const eventRuleId = Number(postData.rule_id) || 0;

  // Page type - always send
  if (wh.rule_type === "page") {
    console.log("Optin page webhook");
    return true;
  }

  // Global type - always send
  if (wh.rule_type === "glob") {
    console.log("Global webhook");
    return true;
  }

  // Rule 1: Standard purchase
  if (wh.rule_id === 1 && cartAbondSkip === 0 && eventRuleId !== 9) {
    if (wh.pricing_id === 0) {
      return true;
    }
    if (wh.pricing_id != 0 &&
      postData.pricing_id &&
      wh.pricing_id == postData.pricing_id[0]) {
      return true;
    }
  }

  // Rule 5: Bump offer
  if (wh.rule_id === 5 && cartAbondSkip === 0 && rlBumpPid.length > 0) {
    if (wh.pricing_id === 0) {
      return true;
    }
    if (wh.pricing_id != 0 && rlBumpPid.indexOf(wh.pricing_id) > -1) {
      return true;
    }
  }

  // Rule 3: Cart abandonment
  if (wh.rule_id === 3 && wh.pricing_id === 0 && cartAbondSkip === 1) {
    return true;
  }

  // Rule 9: manually added member. A purchase arrives with rule_id 1 and must not
  // satisfy this branch — that is what put purchase entries in the manual-member
  // sheet. Manual-member rows carry rule_id 9; rows from before that was stamped
  // carry none, and still match.
  if (
    wh.rule_id === 9 &&
    wh.pricing_id === 0 &&
    cartAbondSkip === 0 &&
    eventRuleId !== 1
  ) {
    return true;
  }

  return false;
}

/**
 * Process individual webhook with proper timeout and error handling
 */
async function processWebhook(wh, subscriberData, paymentForm, tabPrefix) {
  const webhookId = wh.product_rule_id || wh.webhook_name || 'unknown';
  console.log(`[Webhook ${webhookId}] Starting processing...`);

  const dateTime = new Date().toISOString().replace("T", " ").substring(0, 19);
  const startTime = Date.now();

  try {
    let webhookUrl = wh.webhook_url;

    // Fetch webhook URL from database if needed
    if (wh.product_rule_id && !webhookUrl) {
      const webhookParams = {
        TableName: tabPrefix + "product_rule_webhook",
        Key: {
          product_rule_id: wh.product_rule_id
        }
      };

      const getCommand = new GetCommand(webhookParams);
      const webhookResult = await docClient.send(getCommand);

      if (!webhookResult.Item?.webhook_url) {
        console.log(`[Webhook ${webhookId}] No webhook URL found in database`);
        return { success: false, reason: 'no_url' };
      }

      webhookUrl = webhookResult.Item.webhook_url;
    }

    if (!webhookUrl || webhookUrl === "null") {
      console.log(`[Webhook ${webhookId}] No valid webhook URL`);
      return { success: false, reason: 'invalid_url' };
    }

    console.log(`[Webhook ${webhookId}] Calling: ${webhookUrl}`);

    // Call webhook with timeout handling
    const response = await callWebhookWithTimeout(webhookUrl, paymentForm, 30000);

    const duration = Date.now() - startTime;
    console.log(`[Webhook ${webhookId}] Completed in ${duration}ms with status ${response.status}`);

    // Log to DynamoDB
    // await logWebhookCall(
    //   tabPrefix,
    //   subscriberData,
    //   paymentForm,
    //   webhookUrl,
    //   response,
    //   duration,
    //   dateTime
    // );

    return {
      success: response.success,
      status: response.status,
      duration,
      webhookId
    };

  } catch (error) {
    const duration = Date.now() - startTime;
    console.error(`[Webhook ${webhookId}] Error after ${duration}ms:`, error.message);

    // Log error to DynamoDB
    // await logWebhookError(
    //   tabPrefix,
    //   subscriberData,
    //   webhookUrl || 'unknown',
    //   error.message,
    //   dateTime
    // );

    return {
      success: false,
      error: error.message,
      duration,
      webhookId
    };
  }
}

/**
 * Call webhook with timeout using native https module
 */
function callWebhookWithTimeout(webhookUrl, payload, timeout = 30000, extraHeaders = {}) {
  return new Promise((resolve) => {
    try {
      const parsedUrl = new URL(webhookUrl);
      const isHttps = parsedUrl.protocol === 'https:';
      const client = isHttps ? https : http;

      const data = JSON.stringify(payload);

      const options = {
        hostname: parsedUrl.hostname,
        port: parsedUrl.port || (isHttps ? 443 : 80),
        path: parsedUrl.pathname + parsedUrl.search,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(data),
          'User-Agent': 'FlexiFunnels-Webhook/2.0',
          ...extraHeaders
        },
        timeout: timeout
      };

      const req = client.request(options, (res) => {
        let responseBody = '';

        res.on('data', (chunk) => {
          responseBody += chunk;
        });

        res.on('end', () => {
          console.log(responseBody);
          resolve({
            success: res.statusCode >= 200 && res.statusCode < 300,
            status: res.statusCode,
            headers: res.headers,
            body: responseBody
          });
        });
      });

      req.on('error', (err) => {
        console.error('Webhook request error:', err.message);
        resolve({
          success: false,
          status: 0,
          error: err.message
        });
      });

      req.on('timeout', () => {
        req.destroy();
        console.error('Webhook timeout');
        resolve({
          success: false,
          status: 0,
          error: `Timeout after ${timeout}ms`
        });
      });

      req.write(data);
      req.end();

    } catch (error) {
      resolve({
        success: false,
        status: 0,
        error: error.message
      });
    }
  });
}

/**
 * Log successful webhook call to DynamoDB
 */
async function logWebhookCall(tabPrefix, subscriberData, paymentForm, webhookUrl, response, duration, dateTime) {
  try {
    const logParams = {
      TableName: tabPrefix + "autoresponder_data",
      Item: {
        subscriber_id: generateUniqueId(),
        core_subscriber_id: subscriberData.subscriber_id,
        product_id: subscriberData.product_id || 0,
        form_id: subscriberData.form_id || '',
        email: subscriberData.email || '',
        data: paymentForm,
        list: webhookUrl,
        autoresponder_name: "webhook",
        user_agent: subscriberData.user_agent || '',
        created_at: dateTime,
        updated_at: dateTime,
        retry: 1,
        send: response.success ? 1 : 0,
        autoresponder: "webhook",
        contact_status: response.status,
        error_details: response.success ? "" : (response.error || `HTTP ${response.status}`),
        duration_ms: duration
      }
    };

    const putCommand = new PutCommand(logParams);
    await docClient.send(putCommand);
    console.log('Webhook call logged to DynamoDB');
  } catch (error) {
    console.error('Failed to log webhook call:', error);
    // Don't throw - logging failure shouldn't break webhook processing
  }
}

/**
 * Log failed webhook call to DynamoDB
 */
async function logWebhookError(tabPrefix, subscriberData, webhookUrl, errorMessage, dateTime) {
  try {
    const errorParams = {
      TableName: tabPrefix + "autoresponder_data",
      Item: {
        subscriber_id: generateUniqueId(),
        core_subscriber_id: subscriberData.subscriber_id,
        email: subscriberData.email || '',
        product_id: subscriberData.product_id || 0,
        list: webhookUrl,
        autoresponder_name: "webhook",
        created_at: dateTime,
        send: 0,
        error_details: errorMessage,
        contact_status: 0
      }
    };

    const errorPutCommand = new PutCommand(errorParams);
    await docClient.send(errorPutCommand);
    console.log('Webhook error logged to DynamoDB');
  } catch (error) {
    console.error('Failed to log webhook error:', error);
    // Don't throw - logging failure shouldn't break webhook processing
  }
}

/**
 * Generate unique ID for DynamoDB records
 */
const generateUniqueId = () =>
  `_${Date.now().toString(36)}${Math.floor(
    Number.MAX_SAFE_INTEGER * Math.random()
  ).toString(36)}`;

const RETRY_BASE_DELAYS_SEC = [120, 780, 900];
const MAX_DELIVERY_ATTEMPTS = RETRY_BASE_DELAYS_SEC.length + 1;
const MAX_RETRY_AFTER_SEC = 900;
const SUBSCRIPTION_DELIVERY_TIMEOUT_MS = 15000;
const RETRY_SCHEDULE_GROUP = process.env.WEBHOOK_RETRY_GROUP || "webhook_retry";

function jitteredDelaySec(baseDelaySeconds) {
  const jitterFactor = 0.8 + Math.random() * 0.4;
  return Math.max(30, Math.round(baseDelaySeconds * jitterFactor));
}

function shortHash(value) {
  return crypto
    .createHash("sha1")
    .update(String(value))
    .digest("hex")
    .substring(0, 16);
}

function signPayload(signingSecret, timestampSeconds, rawBody) {
  return crypto
    .createHmac("sha256", signingSecret)
    .update(`${timestampSeconds}.${rawBody}`)
    .digest("hex");
}

function parseRuleData(rawRuleData) {
  try {
    const parsed =
      typeof rawRuleData === "string"
        ? JSON.parse(rawRuleData || "{}")
        : rawRuleData || {};
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch (error) {
    return {};
  }
}

async function hydrateRuleIfNeeded(rule, tablePrefix) {
  const alreadyComplete =
    rule.data !== undefined && rule.price_type !== undefined;
  if (alreadyComplete) {
    return rule;
  }
  try {
    const baseItem = await docClient.send(
      new GetCommand({
        TableName: tablePrefix + "product_rules",
        Key: { product_rule_id: rule.product_rule_id },
      })
    );
    if (baseItem?.Item) {
      console.log(`[webhook.sub.hydrated] rule=${rule.product_rule_id}`);
      return { ...rule, ...baseItem.Item };
    }
  } catch (error) {
    console.error(
      `[webhook.sub.hydrate_failed] rule=${rule.product_rule_id} action=rule_skipped error=${error?.message}`
    );
  }
  return rule;
}

function matchesSubscriptionEvent(rule, eventRuleId, subscriptionPricingId) {
  const isDedicatedEventRule =
    Number(rule.rule_id) === Number(eventRuleId);
  if (isDedicatedEventRule) {
    return true;
  }

  const isPurchaseRule = Number(rule.rule_id) === 1;
  if (!isPurchaseRule) {
    return false;
  }

  const ruleConfig = parseRuleData(rule.data);
  const subscribedEventIds = Array.isArray(ruleConfig.subscription_events)
    ? ruleConfig.subscription_events.map(Number)
    : [];
  if (!subscribedEventIds.includes(Number(eventRuleId))) {
    return false;
  }

  const isScopedToOnePricing = Number(rule.price_type) === 2;
  if (isScopedToOnePricing) {
    return (
      subscriptionPricingId != null &&
      Number(rule.pricing_id) === Number(subscriptionPricingId)
    );
  }
  return true;
}

function retryAfterSeconds(responseHeaders) {
  const headerValue =
    responseHeaders &&
    (responseHeaders["retry-after"] || responseHeaders["Retry-After"]);
  if (!headerValue) {
    return null;
  }

  const delaySeconds = parseInt(headerValue, 10);
  if (Number.isFinite(delaySeconds) && delaySeconds > 0) {
    return Math.min(delaySeconds, MAX_RETRY_AFTER_SEC);
  }

  const retryAtMs = Date.parse(headerValue);
  if (!Number.isNaN(retryAtMs)) {
    const secondsUntilRetry = Math.round((retryAtMs - Date.now()) / 1000);
    return secondsUntilRetry > 0
      ? Math.min(secondsUntilRetry, MAX_RETRY_AFTER_SEC)
      : null;
  }
  return null;
}

async function scheduleSubscriptionRetry({
  detail,
  productRuleId,
  nextAttemptNumber,
  delaySeconds,
  targetLambdaArn,
}) {
  const schedulerRoleArn = process.env.SCHEDULER_ROLE_ARN;
  if (!schedulerRoleArn || !targetLambdaArn) {
    console.log(
      `[webhook.retry.skip] reason=scheduler_not_configured rule=${productRuleId}`
    );
    return { scheduled: false };
  }

  const safeDelaySeconds =
    Number.isFinite(delaySeconds) && delaySeconds > 0 ? delaySeconds : 120;
  const runAt = new Date(Date.now() + safeDelaySeconds * 1000);
  const scheduleExpression = `at(${runAt.toISOString().substring(0, 19)})`;
  const scheduleName = `whr-${shortHash(
    `${detail.subscriptionEvent.event_id}:${productRuleId}`
  )}-${nextAttemptNumber}`;

  const scheduleInput = {
    detail: {
      environment: detail.environment,
      subscriptionEvent: detail.subscriptionEvent,
      retry: { attempt: nextAttemptNumber, product_rule_id: productRuleId },
    },
  };

  try {
    const { SchedulerClient, CreateScheduleCommand } = await import(
      "@aws-sdk/client-scheduler"
    );
    const scheduler = new SchedulerClient({});
    await scheduler.send(
      new CreateScheduleCommand({
        Name: scheduleName,
        GroupName: RETRY_SCHEDULE_GROUP,
        ScheduleExpression: scheduleExpression,
        ScheduleExpressionTimezone: "UTC",
        FlexibleTimeWindow: { Mode: "OFF" },
        ActionAfterCompletion: "DELETE",
        Target: {
          Arn: targetLambdaArn,
          RoleArn: schedulerRoleArn,
          Input: JSON.stringify(scheduleInput),
          RetryPolicy: {
            MaximumEventAgeInSeconds: 3600,
            MaximumRetryAttempts: 3,
          },
        },
      })
    );
    console.log(
      `[webhook.retry.scheduled] rule=${productRuleId} attempt=${nextAttemptNumber} in=${safeDelaySeconds}s at=${scheduleExpression}`
    );
    return { scheduled: true, name: scheduleName };
  } catch (error) {
    if (error?.name === "ConflictException") {
      console.log(
        `[webhook.retry.scheduled] rule=${productRuleId} attempt=${nextAttemptNumber} result=already_exists`
      );
      return { scheduled: true, name: scheduleName, duplicate: true };
    }
    console.error(
      `[webhook.retry.failed] rule=${productRuleId} attempt=${nextAttemptNumber} error=${error?.message}`
    );
    return { scheduled: false, error: error?.message };
  }
}

async function logSubscriptionDelivery({
  tablePrefix,
  detail,
  rule,
  attemptNumber,
  deliveryResult,
  durationMs,
  acceptedViaAppsScript,
}) {
  const accepted = Boolean(deliveryResult.success || acceptedViaAppsScript);
  const subscriptionEvent = detail.subscriptionEvent;
  const loggedAt = new Date().toISOString().replace("T", " ").substring(0, 19);
  const customerEmail = subscriptionEvent.envelope?.data?.customer?.email || "";
  try {
    await docClient.send(
      new PutCommand({
        TableName: tablePrefix + "autoresponder_data",
        Item: {
          subscriber_id: generateUniqueId(),
          core_subscriber_id: subscriptionEvent.event_id,
          product_id: Number(subscriptionEvent.product_id) || 0,
          form_id: "",
          ...(customerEmail ? { email: customerEmail } : {}),
          data: subscriptionEvent.envelope,
          list: rule.webhook_url,
          autoresponder_name: "webhook",
          user_agent: "",
          created_at: loggedAt,
          updated_at: loggedAt,
          retry: attemptNumber,
          send: accepted ? 1 : 0,
          autoresponder: "webhook",
          contact_status: deliveryResult.status,
          accepted_redirect: acceptedViaAppsScript ? 1 : 0,
          error_details: accepted
            ? ""
            : String(
                deliveryResult.error || `HTTP ${deliveryResult.status}`
              ).substring(0, 500),
          duration_ms: durationMs,
          event_name: subscriptionEvent.event_name,
          product_rule_id: rule.product_rule_id,
        },
      })
    );
  } catch (error) {
    console.error(
      `[webhook.sub.log_failed] rule=${rule.product_rule_id} event=${subscriptionEvent.event_id} url=${rule.webhook_url} status=${deliveryResult.status} send=${accepted ? 1 : 0} error=${error?.message}`
    );
  }
}

async function deliverSubscriptionWebhook(rule, envelope) {
  const timestampSeconds = Math.floor(Date.now() / 1000);
  const requestHeaders = {
    "X-FF-Event": envelope.type,
    "X-FF-Event-Id": envelope.id,
    "X-FF-Timestamp": String(timestampSeconds),
  };

  const amount = envelope.data?.amount || envelope.data?.charged_amount || "";
  const body = {
    ...envelope,
    event: envelope.type,
    name: envelope.data?.customer?.name,
    first_name: envelope.data?.customer?.name,
    email: envelope.data?.customer?.email,
    phone: envelope.data?.customer?.phone,
    product_title: envelope.product_name,
    amount,
    price: amount,
    currency: envelope.data?.currency,
    status: envelope.data?.status,
    order_id: envelope.data?.order_id,
    orderId: envelope.data?.order_id,
    transaction_id: envelope.data?.payment_id,
    subscription_id: envelope.data?.subscription_id,
    plan_name: envelope.data?.plan_name,
    ...vendorBillingInterval(
      envelope.data?.billing_interval,
      envelope.data?.billing_interval_count
    ),
    billing_frequency: billingFrequencyLabel(
      envelope.data?.billing_interval,
      envelope.data?.billing_interval_count
    ),
    billing_cycle: envelope.data?.billing_cycle,
    next_billing_date: ["canceled", "cancelled", "expired", "refunded"].includes(
      String(envelope.data?.status || "").toLowerCase()
    )
      ? undefined
      : envelope.data?.current_period_end,
    datetime: new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" }),
  };

  const signingSecret = parseRuleData(rule.data).webhook_secret;
  if (signingSecret) {
    requestHeaders["X-FF-Signature"] = `v1=${signPayload(
      signingSecret,
      timestampSeconds,
      JSON.stringify(body)
    )}`;
  }

  return callWebhookWithTimeout(
    rule.webhook_url,
    body,
    SUBSCRIPTION_DELIVERY_TIMEOUT_MS,
    requestHeaders
  );
}

export async function handleSubscriptionEvent(detail, context) {
  const tablePrefix = detail.environment === "dev" ? "dev" : "";
  const subscriptionEvent = detail.subscriptionEvent;
  const retryContext = detail.retry || null;
  const attemptNumber = retryContext
    ? Number(retryContext.attempt) || 1
    : 1;

  const isValidPayload =
    subscriptionEvent?.product_id &&
    subscriptionEvent?.rule_id &&
    subscriptionEvent?.envelope &&
    subscriptionEvent?.event_id;

  if (!isValidPayload) {
    console.error("[webhook.sub] invalid payload");
    return { statusCode: 400, error: "invalid subscriptionEvent payload" };
  }

  console.log(
    `[webhook.sub] event=${subscriptionEvent.event_name} rule_id=${subscriptionEvent.rule_id} product=${subscriptionEvent.product_id} attempt=${attemptNumber}`
  );

  const rulesQuery = new QueryCommand({
    TableName: tablePrefix + "product_rules",
    IndexName: "product_id-index",
    KeyConditionExpression: "product_id = :v2",
    FilterExpression: "#type_of_flow = :v3",
    ExpressionAttributeValues: {
      ":v2": Number(subscriptionEvent.product_id),
      ":v3": "webhook",
    },
    ExpressionAttributeNames: { "#type_of_flow": "autoresponder" },
  });

  let rulesResult;
  try {
    rulesResult = await docClient.send(rulesQuery);
  } catch (error) {
    console.error(
      `[webhook.sub.query_failed] event=${subscriptionEvent.event_id} type=${subscriptionEvent.event_name} product=${subscriptionEvent.product_id} error=${error?.message}`
    );
    throw error;
  }

  const deliverableRules = (rulesResult.Items || []).filter(
    (rule) =>
      rule.webhook_url &&
      rule.webhook_url !== "null" &&
      String(rule.webhook_url).startsWith("http")
  );

  const hydratedResults = await Promise.allSettled(
    deliverableRules.map((rule) => hydrateRuleIfNeeded(rule, tablePrefix))
  );
  const hydratedRules = hydratedResults.map((result, index) =>
    result.status === "fulfilled" ? result.value : deliverableRules[index]
  );

  let matchingRules = hydratedRules.filter((rule) =>
    matchesSubscriptionEvent(
      rule,
      subscriptionEvent.rule_id,
      subscriptionEvent.pricing_id
    )
  );

  if (retryContext?.product_rule_id) {
    matchingRules = matchingRules.filter(
      (rule) =>
        String(rule.product_rule_id) === String(retryContext.product_rule_id)
    );
  }

  if (matchingRules.length === 0) {
    console.log(
      `[webhook.sub] event=${subscriptionEvent.event_name} rules=0`
    );
    return { statusCode: 200, message: "no matching rules" };
  }

  const selfLambdaArn =
    context?.invokedFunctionArn || process.env.SELF_LAMBDA_ARN;

  const settledOutcomes = await Promise.allSettled(
    matchingRules.map(async (rule) => {
      const startedAt = Date.now();
      const deliveryResult = await deliverSubscriptionWebhook(
        rule,
        subscriptionEvent.envelope
      );
      const durationMs = Date.now() - startedAt;

      const redirectLocation = String(deliveryResult.headers?.location || "");
      const acceptedViaAppsScript =
        deliveryResult.status === 302 &&
        (redirectLocation.startsWith("https://script.googleusercontent.com/") ||
          String(rule.webhook_url).startsWith(
            "https://script.google.com/macros/"
          ));

      await logSubscriptionDelivery({
        tablePrefix,
        detail,
        rule,
        attemptNumber,
        deliveryResult,
        durationMs,
        acceptedViaAppsScript,
      });

      if (deliveryResult.success || acceptedViaAppsScript) {
        console.log(
          `[webhook.sub.delivered] rule=${rule.product_rule_id} status=${deliveryResult.status} attempt=${attemptNumber} duration_ms=${durationMs}`
        );
        return "delivered";
      }

      const isPermanentlyGone = deliveryResult.status === 410;
      const attemptsExhausted = attemptNumber >= MAX_DELIVERY_ATTEMPTS;

      if (isPermanentlyGone || attemptsExhausted) {
        console.error(
          `[webhook.dead] rule=${rule.product_rule_id} event=${subscriptionEvent.event_id} url=${rule.webhook_url} attempts=${attemptNumber} reason=${
            isPermanentlyGone ? "gone_410" : "attempts_exhausted"
          } last_error=${deliveryResult.error || `status ${deliveryResult.status}`}`
        );
        return "dead";
      }

      const vendorRequestedDelay = retryAfterSeconds(deliveryResult.headers);
      const nextDelaySeconds =
        vendorRequestedDelay != null
          ? vendorRequestedDelay
          : jitteredDelaySec(RETRY_BASE_DELAYS_SEC[attemptNumber - 1]);

      const scheduleResult = await scheduleSubscriptionRetry({
        detail,
        productRuleId: rule.product_rule_id,
        nextAttemptNumber: attemptNumber + 1,
        delaySeconds: nextDelaySeconds,
        targetLambdaArn: selfLambdaArn,
      });

      if (scheduleResult.scheduled) {
        return "scheduled";
      }

      console.error(
        `[webhook.dead] rule=${rule.product_rule_id} event=${subscriptionEvent.event_id} url=${rule.webhook_url} attempts=${attemptNumber} reason=retry_scheduling_failed last_error=${scheduleResult.error || "unknown"}`
      );
      return "dead";
    })
  );

  const outcomes = settledOutcomes.map((result, index) => {
    if (result.status === "fulfilled") {
      return result.value;
    }
    console.error(
      `[webhook.dead] rule=${matchingRules[index]?.product_rule_id} event=${subscriptionEvent.event_id} url=${matchingRules[index]?.webhook_url} attempts=${attemptNumber} reason=delivery_threw last_error=${result.reason?.message}`
    );
    return "dead";
  });

  const deliveredCount = outcomes.filter((o) => o === "delivered").length;
  const scheduledCount = outcomes.filter((o) => o === "scheduled").length;
  const deadCount = outcomes.filter((o) => o === "dead").length;

  console.log(
    `[webhook.sub.done] event=${subscriptionEvent.event_name} delivered=${deliveredCount} scheduled=${scheduledCount} dead=${deadCount}`
  );

  return {
    statusCode: 200,
    message: `delivered ${deliveredCount}, retry ${scheduledCount}, dead ${deadCount}`,
    details: {
      delivered: deliveredCount,
      scheduled: scheduledCount,
      dead: deadCount,
    },
  };
}

function vendorBillingInterval(interval, intervalCount) {
  const frequencyLabel = billingFrequencyLabel(interval, intervalCount);
  if (!frequencyLabel || frequencyLabel.startsWith("every ")) {
    return { billing_interval: interval, billing_interval_count: intervalCount };
  }
  return { billing_interval: frequencyLabel, billing_interval_count: 1 };
}
