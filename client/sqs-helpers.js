var amazonKeys = require('./config');
const easy = require('easy-sqs');
const awsConfig = {
  'accessKeyId': amazonKeys.sqsConfig.accessKeyId,
  'secretAccessKey': amazonKeys.sqsConfig.secretAccessKey,
  'region': amazonKeys.sqsConfig.region
};
const url = 'https://sqs.us-east-1.amazonaws.com/410939018954/ToyQ';
const client = easy.createClient(awsConfig);

exports.sendMessage = (url, message) => {
  client.getQueue(url, (err, queue) => {
    if (err) console.log('Queue does not exist.');
    let msg = JSON.stringify({body: message});
    queue.sendMessage(msg, function(err) { if (err) console.error(err)});
  });
};
