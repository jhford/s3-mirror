CREATE TABLE IF NOT EXISTS artifacts (
  region TEXT NOT NULL, -- S3 region where the copy will be made to, e.g. us-east-1
  bucket TEXT NOT NULL, -- bucket name of the original artifact, e.g. taskcluster-public-artifacts
  object TEXT NOT NULL, -- object name of the original artifact, e.g. public/build/target.tar.gz
  state TEXT NOT NULL DEFAULT 'pending', -- state of object, e.g. pending or present
  requested TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- time when inserted into DB
  started TIMESTAMPTZ, -- time when the copy was started
  completed TIMESTAMPTZ, -- time when the copy was completed
  PRIMARY KEY(region, bucket, object)
)
