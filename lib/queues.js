const assert = require('assert');
const {QueueSender, QueueListener, getQueueUrl} = require('sqs-simple');

const cfg = require('typed-env-config')();

let queueName = cfg.queues.queueName;

module.exports.getSender = async function({sqs, queueName}) {
  assert(typeof sqs === 'object', 'bad sqs client');
  assert(typeof queueName === 'string', 'bad queueName');
  let queueUrl = await getQueueUrl({sqs, queueName})
  let sender = new QueueSender({sqs, queueUrl, encodeMessage: true});
  return sender;
}

module.exports.createListener = async function({sqs, queueName, handler}) {
  assert(typeof sqs === 'object', 'bad sqs client');
  assert(typeof queueName === 'string', 'bad queueName');
  assert(typeof sqs === 'object', 'bad sqs client');
  assert(typeof handler === 'function', 'bad handler');

  let queueUrl = await getQueueUrl({sqs, queueName})

  listener = new QueueListener({
    sqs,
    queueUrl,
    handler,
    decodeMessages: true,
    sequential: false,
    visibilityTimeout: cfg.queues.visibilityTimeout,
    waitTimeSeconds: cfg.queues.waitTimeSeconds,
    maxNumberOfMessages: cfg.queues.maxNumberOfMessages,
  });
  
  // Set up the event listeners for logging info
  listener.on('starting', () => {
    log.info({queueName, queueUrl}, 'starting to listen'); 
  });
  listener.on('stopped', () => {
    log.info({queueName, queueUrl}, 'stopped listening'); 
  });

  // TODO: Consider whether we should listen for errors on the listener
  // here, and if so how to handle api, handler or payload

  return listener;
}
