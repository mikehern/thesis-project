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
    const payload = JSON.parse(message.Body);
    
    let uncachedLocations = [];
    console.log('uncachedLocations: ', uncachedLocations);
    
    //Filter uncached locations, to be eventually put on experiences Q.
    
    Promise.mapSeries(payload.recent, (location) => {
      return cache
        .llenAsync(`${location}:results`)
        .then(result => {
          if (result === 0) {
            uncachedLocations.push(location);
            console.log('uncachedLocations: ', uncachedLocations);
          }
        })
        .catch(err => pino.error(new Error({ route: '', method: '', stage: 'MIDDLE', worker: 'userHistory' }, 'during recent cache')));
    })

      //Clears the user's cache first, then updates it with latest history
      //Sends one location at a time, for now.

      .then(() => cache.delAsync(`${payload.userId}:results`))
      .then(() => cache.lpushAsync(`${payload.userId}:results`, payload.recent))
      .then(() => {
        pino.info({ route: '', method: '', stage: 'MIDDLE', worker: 'userHistory' }, 'after cache, before SQS send');

        //sync

        // uncachedLocations.forEach((location) => {
        //   const params = {
        //     MessageAttributes: {
        //       serviceOrigin: {
        //         DataType: 'String',
        //         StringValue: 'Client'
        //       },
        //       leftServiceAt: {
        //         DataType: 'String',
        //         StringValue: `${TSNow}`
        //       }
        //     },
        //     MessageBody: JSON.stringify(location),
        //     QueueUrl: EXPERIENCES_Q_INBOUND
        //   };

        //   sqs.sendMessage(params).promise()
        //     .then(() => pino.info({ route: '', method: '', stage: 'MIDDLE', worker: 'userHistory' }, `sqs: ${location}`))
        //     .catch(err => pino.error(new Error({ route: '', method: '', stage: 'MIDDLE', worker: 'userHistory' }, 'during sqs send')));
        // });

        //async promisemapped

        return Promise.mapSeries(uncachedLocations, (location) => {
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

          return sqs.sendMessage(params).promise()
            .then(() => pino.info({ route: '', method: '', stage: 'MIDDLE', worker: 'userHistory' }, `sqs: ${location}`));
        });     
      })
      .then(() => done())
      .then(() => pino.info({ route: '', method: '', stage: 'END', worker: 'userHistory' }))
      .catch(err => pino.error(new Error({ route: '', method: '', stage: 'MIDDLE', worker: 'userHistory' }, 'during sqs prep')));
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