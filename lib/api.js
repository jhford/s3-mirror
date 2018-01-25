const log = require('./log');
const express = require('express');

const port = process.env.PORT || 8080;

const app = express();

app.get('/:region/:bucket/:object', async (req, res) => {
  let region = req.params.region;
  let bucket = req.params.bucket;
  let object = req.params.object;

  log.info({region, bucket, object}, 'requesting object');
  res.send();
});

app.delete('/:region/:bucket/:object', async (req, res) => {
  let region = req.params.region;
  let bucket = req.params.bucket;
  let object = req.params.object;

  log.info({region, bucket, object}, 'purging object');
  res.send();
});

app.listen(process.env.PORT || 8080, () => {
  log.info({port}, 'listening');
});
