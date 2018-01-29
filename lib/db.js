const PG = require('pg');
const Pool = require('pg-pool');
const cfg = require('typed-env-config')();
const assert = require('assert');
const url = require('url');

PG.types.setTypeParser(20, x => parseInt(x, 10));

PG.types.setTypeParser(1184, x => {
  return new Date(x);
});

module.exports.db = function () {

  const dburl = cfg.postgres.databaseUrl;
  assert(dburl, 'Must have a DATABASE_URL value');
  let parsedUrl = url.parse(dburl);
  let [user, password] = parsedUrl.auth.split([':']);

  //let client = PG.native ? PG.native.Client : PG.Client;
  let client = PG.client;

  let poolcfg = {
    user,
    password,
    database: parsedUrl.pathname.replace(/^\//, ''),
    port: Number.parseInt(parsedUrl.port, 10),
    host: parsedUrl.hostname,
    application_name: 'ec2_manager',
    max: cfg.postgres.maxClients || 20,
    min: cfg.postgres.minClients || 4,
    idleTimeoutMillis: cfg.postgres.idleTimeoutMillis,
    maxWaitingClients: 50,
    Client: client,
  };

  let pool = new Pool(poolcfg);

  // Per readme, this is a rare occurence but should be at least logged.
  // Basically, this error event is emitted when a client emits an error
  // while not claimed by any process.
  pool.on('error', (err, client) => {
    log.error(err, 'Postgres Pool error');
  });

  return pool;
}
