let log = require('taskcluster-lib-log');

let env = process.env.NODE_ENV;
if (!env) {
  env = 'development';
}

module.exports = log('s3-mirror-' + env.trim());
