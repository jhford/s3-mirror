const assert = require('assert');
const log = require('./log');
const express = require('express');
const aws = require('aws-sdk');
const queues = require('./queues');

const cfg = require('typed-env-config')();
const redis = require('redis');

const {serializeKey, parseKey} = require('./cache');

const port = process.env.PORT || 8080;

const app = express();

let sqsConfig = cfg.sqs;
let sqsDebugBridge = {
  write: x => {
    log.debug(x);
  },
};

let sqs = new aws.SQS(sqsConfig);
let redisClient = redis.createClient(cfg.redis);

// We need these references for the API handlers later.  These are initialised
// in the start() method below
let queueSender;

// We're going to use Redis as our data store for synchronization

async function requestSource({ip}) {
  return {service: 's3', region: 'us-east-1'};
};

// Request a resource
app.get('/:region/:bucket/:object/:error*?', async (req, res) => {

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
  let source;
  try {
    source = await requestSource({ip: req.ip});
  } catch (err) {
    req.status(500).send('cannot determine source of request\n');
    log.error(err, 'determining location');
    return;
  }

  let msg = {
    action: 'insert',
    region: req.params.region,
    bucket: req.params.bucket,
    object: req.params.object,
    // The object should be copied into the region that this request
    // originated from
    destination: source,
  };

  // Insert the item into the queue for copying
  try {
    await queueSender.insert(msg);
  } catch (err) {
    log.error(err, 'requesting transfer of object');
    res.status(500).end();
    return;
  }

  try {
    let key = serializeKey(msg);
    let result = await new Promise((resolve, reject) => {
      redisClient
        .multi()
        .set(key, 'pending')
        .expire(key, cfg.cache.defaultExpiration / 1000)
        .exec((err, result) => {
          if (err) {
            return reject(err);
          }
          return resolve(result);
        });
    });
    log.info(msg, 'requested copy');
    return res.status(204).end();
  } catch (err) {
    log.error(err, 'inserting into redis');
    res.status(500).end();
    return;
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
  // This will be used to enqueue requests to cache
  queueSender = await queues.getSender({sqs, queueName: cfg.queues.queueName});

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

