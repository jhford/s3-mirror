const log = require('../lib/log');

// Set up a DB pool for the API
const DB = require('../lib/db');
const db = DB.db();

const {copy} = require('../lib/s3');

async function poll() {
  let client = await db.connect();

  // Start a transaction
  try {
    await client.query('BEGIN');
  } catch (err) {
    client.release();
    throw err;
  }

  let result;
  try {
    result = await client.query({
      text: "SELECT region, bucket, object FROM artifacts WHERE state = 'pending' AND started IS NULL ORDER BY requested LIMIT 1 FOR UPDATE;",
      values: [],
    });
  } catch (err) {
    await client.query('ROLLBACK;');
    client.release();
    throw err;
  }

  // If there's nothing pending, let's sleep for a while
  if (result.rowCount === 0) {
    try {
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }

    await new Promise(x => {
      setTimeout(x, 500);
    });

  } else {
    // Claim this artifact copy
    let artifact = result.rows[0];
    try {
      await client.query({
        text: "UPDATE artifacts SET started = $1 WHERE region = $2 AND bucket = $3 AND object = $4;",
        values: [new Date(), artifact.region, artifact.bucket, artifact.object],
      });
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK;');
      throw err;
    } finally {
      client.release();
    }

    // Do the copy
    try {
      await copy({region: artifact.region, bucket: artifact.bucket, object: artifact.object});
    } catch (err) {
      await db.query({
        text: "UPDATE artifacts SET started = $1, state = 'pending' WHERE region = $2 AND bucket = $3 AND object = $4;",
        values: [null, artifact.region, artifact.bucket, artifact.object],
      });
      throw err;
    }

    await db.query({
      text: "UPDATE artifacts SET state = 'present', completed = $1 WHERE region = $2 AND bucket = $3 AND object = $4;",
      values: [new Date(), artifact.region, artifact.bucket, artifact.object],
    });

  }
}

async multiPoll(n) {
  

}


async function start() {
  while (true) {
    try {
      await pollToCopy();
    } catch (err) {
      log.error(err, 'polling error');
    }
  }
}

if (!module.parent) {
  start().catch(err => {
    console.log(err.stack || err);
  });
}

