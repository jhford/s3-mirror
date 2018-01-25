const cfg = require('typed-env-config')();

function createS3Url({region, bucket, object}) {
  assert(typeof region === 'string', 'missing region');
  assert(typeof bucket === 'string', 'missing bucket');
  assert(typeof object === 'string', 'missing object');

  region = region === 'us-east-1' ? 's3' : 's3-' + region;
  let url = `https://${bucket}.${region}.amazonaws.com/${object}`;
}
module.exports = createS3Url;
