const axios = require('axios');
const moment = require('moment');
const Promise = require('bluebird');
const pino = require('pino')();
const AWS = require('aws-sdk');
const AWScredentials = require('./config');
AWS.config.update({
  region: AWScredentials.sqsConfig.region,
  accessKeyId: AWScredentials.sqsConfig.accessKeyId,
  secretAccessKey: AWScredentials.sqsConfig.secretAccessKey
});
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });
const Consumer = require('sqs-consumer');

const redis = require('redis');
const cache = redis.createClient();
Promise.promisifyAll(redis.RedisClient.prototype);
Promise.promisifyAll(redis.Multi.prototype);
cache.on('connect', () => console.log('Connected to Redis!'));

const USER_Q_SEARCHRESULTS = `https://sqs.us-east-1.amazonaws.com/410939018954/Dummy_USER_Q_SEARCHRESULTS`;
const EXPERIENCES_Q_UPDATES = `https://sqs.us-east-1.amazonaws.com/410939018954/Dummy_EXPERIENCES_Q_UPDATES`;
const EXPERIENCES_Q_INBOUND = `https://sqs.us-east-1.amazonaws.com/410939018954/Dummy_EXPERIENCES_Q_INBOUND`;

const TSNow = moment(Date.now()).format('llll');


//Workers
const userHistoryWorker = Consumer.create({
  queueUrl: USER_Q_SEARCHRESULTS,
  handleMessage: (message, done) => {
    pino.info({ route: '', method: '', stage: 'BEGIN', worker: 'userHistory' }, `${message.Body}`);
    const {userId: userId, userHistory: recent} = message.Body;
    
    let uncachedLocations = [];

    userHistory.forEach((location) => {
      if (cache.llenAsync(`${location}:results`) === 0) {
        uncachedLocations.push(location);
      }
    });

    //Clears the user's cache first, then updates it with latest history
    //Sends one location at a time, for now.

    pino.info({ route: '', method: '', stage: 'MIDDLE', worker: 'userHistory' }, 'after parsing history, before caching history');

    cache
      .delAsync(userId)
      .then(() => cache.lpushAsync(`${userId}:results`, userHistory))
      .then(() => {
        pino.info({ route: '', method: '', stage: 'MIDDLE', worker: 'userHistory' }, 'after cache, before SQS send');
        uncachedLocations.forEach((location) => {
          const params = {
            MessageAttributes: {
              serviceOrigin: {
                DataType: 'String',
                StringValue: 'Client'
              },
              leftServiceAt: {
                DataType: 'String',
                StringValue: `${TSNow}`
              }
            },
            MessageBody: JSON.stringify(location),
            QueueUrl: EXPERIENCES_Q_INBOUND
          };

          sqs.sendMessage(params).promise()
            .then(result => pino.info({ route: '', method: '', stage: 'END', worker: 'userHistory' }, `sqs: ${result}`))
            .catch(err => pino.error(new Error({ route: '', method: '', stage: 'MIDDLE', worker: 'userHistory' }, 'during sqs send')));
        });
      })
      .catch(err => pino.error(new Error({ route: '', method: '', stage: 'MIDDLE', worker: 'userHistory' }, 'during sqs prep')));
    
    //Intentionally making done non-blocking.

    done();
  },

  //Starting with a long wait time
  
  waitTimeSeconds: 10
});

userHistoryWorker.on('empty', data => console.log('UserHistoryQ is empty...'));
userHistoryWorker.on('error', err => console.error('UserHistoryQ has this issue: ', err));
userHistoryWorker.start();



//Expects a payload of locationID-experiences. Takes the experiences and dumps them in cache.

const experiencesWorker = Consumer.create({
  queueUrl: EXPERIENCES_Q_UPDATES,
  handleMessage: (message, done) => {
    pino.info({ route: '', method: '', stage: 'BEGIN', worker: 'experiences' }, `${message.Body}`);

    //TODO: conditionally handle the 2 different types of payloads:
    //1) location-experiences
    //2) user-location-experiences pagination

    const { locationId, locations } = message.Body;

    pino.info({ route: '', method: '', stage: 'MIDDLE', worker: 'experiences' }, 'after parsing history, before caching history');

    cache
      .lpushAsync(`${locationId}:results`, locations)
      .then(() => pino.info({ route: '', method: '', stage: 'END', worker: 'experiences' }))
      .catch(err => pino.error(new Error({ route: '', method: '', stage: 'MIDDLE', worker: 'experiences' }, 'during cache push')));

    //Intentionally making done non-blocking.

    done();
  },

  //Starting with a long wait time

  waitTimeSeconds: 10
});

experiencesWorker.on('empty', data => console.log('ExperiencesQ is empty...'));
experiencesWorker.on('error', err => console.error('ExpereiencesQ has this issue: ', err));
experiencesWorker.start();