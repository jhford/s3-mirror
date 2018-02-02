const log = require('./log');
const assert = require('assert');
const cfg = require('typed-env-config')();

// Construct an S3 URL
function createUrl({region, bucket, object}) {
  assert(typeof region === 'string', 'missing region');
  assert(typeof bucket === 'string', 'missing bucket');
  assert(typeof object === 'string', 'missing object');

  region = region === 'us-east-1' ? 's3' : 's3-' + region;
  let url = `https://${bucket}.${region}.amazonaws.com/${object}`;
  return url;
}

// Translate a source location into a cache location.
function computeCachedLocation({region, bucket, object}) {
  assert(typeof region === 'string', 'missing region');
  assert(typeof bucket === 'string', 'missing bucket');
  assert(typeof object === 'string', 'missing object');

  return {
    region,
    bucket: cfg.bucketConfig[region] || (bucket + '_' + region),
    object,
  };
}


// Given an IP address, return the string which identifies the S3 region which
// contains that IP.  During development, this is hardcoded to 'us-east-1'
function regionForIp({ip}) {
  return 'us-west-1';
}

// Assert that a region is a valid format.  If there's a problem with the
// region, an exception will be raised.  If the region is a valid name, the
// regionname will be returned without changes.  This will not check if the
// region exists or if there are permissions to access it
function assertValidRegion({region}) {
  return region;
}

// Assert that a bucket is a valid format.  If there's a problem with the
// bucket, an exception will be raised.  If the bucket is a valid name, the
// bucketname will be returned without changes.  This will not check if the
// bucket exists or if there are permissions to access it
function assertValidBucket({bucket}) {
  return bucket;
}

// Assert that a object is a valid format.  If there's a problem with the
// object, an exception will be raised.  If the object is a valid name, the
// objectname will be returned without changes.  This will not check if the
// object exists or if there are permissions to access it
function assertValidObject({object}) {
  return object;
}

// Copy an S3 bucket object
async function copy({region, bucket, object}) {
  let sourceRegion = cfg.app.canonicalRegion;
  let dest = computeCachedLocation({region, bucket, object});
  log.info({sourceRegion, region: dest.region, bucket: dest.bucket, object: dest.object}, 'copying'); 
  log.info({sourceRegion, region: dest.region, bucket: dest.bucket, object: dest.object}, 'copyied'); 
}

module.exports.createUrl = createUrl;
module.exports.computeCachedLocation = computeCachedLocation;
module.exports.regionForIp = regionForIp;
module.exports.assertValidRegion = assertValidRegion;
module.exports.assertValidBucket = assertValidBucket;
module.exports.assertValidObject = assertValidObject;
module.exports.copy = copy;
