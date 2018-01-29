const log = require('../lib/log');

const assert = require('assert');
const express = require('express');
const aws = require('aws-sdk');
const {
  createUrl,
  regionForIp,
  assertValidBucket,
  assertValidObject,
  assertValidRegion,
} = require('../lib/s3');

// Access the configuration
const cfg = require('typed-env-config')();

const port = process.env.PORT || 8080;

const app = express();

// Set up a DB pool for the API
const DB = require('../lib/db');
const db = DB.db();

// In order to avoid having to indent this entire request handler in a try
// catch, I've made it a function which will be called by the actual request
// handler which will try/catch it, for error logging
async function handleRedirect(req, res) {
  let region = assertValidRegion({region: req.params.region});
  let bucket = assertValidBucket({bucket: req.params.bucket});
  let object = assertValidObject({object: req.params.object});

  // A truthy value of the error parameter would imply someone more than likely
  // incorrectly omitted url encoding of a resource name.  The default
  // behaviour of issuing a 404 would be safe and correct, but as this is
  // likely to be a common error case, we should handle it with a useful error
  // message
  if (req.params.error) {
    res.status(400).send('ensure url encoding of object name\n');
    return;
  }

  // We need to figure out where this request is coming from.  This will be
  // able to tell us where it's going to end up going in the end
  let requestRegion;
  try {
    requestRegion = await regionForIp({ip: req.ip});
  } catch (err) {
    req.status(500).send('cannot determine source of request\n');
    log.error(err, 'determining location');
    return;
  }

  // Determine the canonical url for the artifact
  let canonicalUrl = createUrl({region, bucket, object});

  // Determine the url of where we're copying to
  let copiedUrl = createUrl({region: requestRegion, bucket: bucket + '-' + requestRegion, object});

  let client = await db.connect();

  // Start a transaction
  try {
    await client.query('BEGIN');
  } catch (err) {
    client.release();
    throw err;
  }

  // Find out if there's an existing copy request or not.
  let result;
  try {
    result = await client.query({
      text: 'SELECT state FROM artifacts WHERE region = $1 AND bucket = $2 AND object = $3 FOR UPDATE;',
      values: [region, bucket, object],
    });
  } catch (err) {
    await client.query('ROLLBACK');
    client.release();
    throw err;
  }

  // If we find that we have the artifact present, we should redirect to it and
  // end this handler.
  if (result.rowCount === 1 && result.rows[0].state === 'present') {
    try {
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      log.info({region, bucket, object, canonicalUrl, copiedUrl}, 'found artifact in cache, redirecting');
      client.release();
      return res.redirect(copiedUrl);
    }
  }

  // If the artifact is *not* in the database, we want to insert it using the
  // same transaction
  if (result.rowCount === 0) {
    try {
      await client.query({
        text: 'INSERT INTO artifacts (region, bucket, object) VALUES ($1, $2, $3);',
        values: [region, bucket, object],
      });
      log.info({region, bucket, object, canonicalUrl, copiedUrl}, 'requesting copy');
    } catch (err) {
      await client.query('ROLLBACK');
      client.release();
      throw err
    }
  }

  // We want to commit the transaction because we no longer need it for anything it.
  try {
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }

  // NOW POLL!  We'll do up to 25 iterations, with a 1s delay between each Note
  // that we're not using the same transaction because we don't want to keep
  // anything locked
  for (let i = 0 ; i < 25 ; i++) {
    let result = await db.query({
      text: 'SELECT state FROM artifacts WHERE region = $1 AND bucket = $2 AND object = $3;',
      values: [region, bucket, object],
    });

    if (result.rowCount === 1) {
      if (result.rows[0].state === 'present') {
        log.info({region, bucket, object, canonicalUrl, copiedUrl, timeInSec: i}, 'polled for and found copied artifact');
        return res.redirect(copiedUrl);
      }
    }

    await new Promise((resolve, reject) => { setTimeout(resolve, 1000) });
  }

  // If we reach here, we didn't have a copy complete in time and need to redirect to the canonical URL
  log.warn({region, bucket, object, canonicalUrl, copiedUrl, timeInSec: 25}, 'failed to copy in time, redirecting to original');
  return res.redirect(canonicalUrl);

}

app.get('/:region/:bucket/:object/:error*?', async (req, res) => {
  try {
    await handleRedirect(req, res);
  } catch (err) {
    log.error(err);
  }
});

app.get('/request-source', async (req, res) => {
  try {
    let result = await requestSource({ip: req.ip});
    res.status(200).send(JSON.stringify(result, null, 2) + '\n');
  } catch (err) {
    log.error(err, 'determining location');
    res.status(500);
  } finally {
    res.end();
  }
});

async function start() {
  await new Promise((res, rej) => {
    app.listen(process.env.PORT || 8080, err => {
      if (err) {
        return rej(err);
      }
      log.info({port}, 'listening');
      return res(err);
    });
  });
}

if (!module.parent) {
  start().catch(err => {
    console.log(err.stack || err);
  });
}

