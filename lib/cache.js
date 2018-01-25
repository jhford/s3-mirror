module.exports.serializeKey = ({region, bucket, key, destination}) => {
  return Buffer.from(JSON.stringify({region, bucket, key, destination}), 'utf-8').toString('base64');
}

module.exports.parseKey = (key) => {
  return JSON.parse(Buffer.from(key, 'base64'));
}

