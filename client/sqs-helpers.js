const Consumer = require('sqs-consumer');
const AWS = require('aws-sdk');
const AWScredentials = require('./config');

// myConfig = new AWS.Config();
AWS.config.update({
  region: AWScredentials.sqsConfig.region,
  accessKeyId: AWScredentials.sqsConfig.accessKeyId,
  secretAccessKey: AWScredentials.sqsConfig.secretAccessKey
});

//FETCHES FROM QUEUE
const app = Consumer.create({
  queueUrl: 'https://sqs.us-east-1.amazonaws.com/410939018954/ToyQ',
  handleMessage: (message, done) => {
    console.log('Q payloads body is: ', message.Body);
    done();
  },
  waitTimeSeconds: 10
});

app.on('empty', data => console.log('queue is empty...', data));

app.on('error', err => console.error(err));

app.start();