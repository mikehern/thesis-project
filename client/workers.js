const axios = require('axios');
const moment = require('moment');
const Promise = require('bluebird');
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

const USER_Q_SEARCHRESULTS = `javi's outbound queue`;
const EXPERIENCES_Q_UPDATES = `aric's outbound queue`;
const EXPERIENCES_Q_INBOUND = `aric's inbound queue`;

const TSNow = moment(Date.now()).format('llll');


//Workers
const userHistoryWorker = Consumer.create({
  queueUrl: USER_Q_SEARCHRESULTS,
  handleMessage: (message, done) => {
    console.log('Fetched from the UserQ: ', message.Body);
    const {userId: userId, userHistory: recent} = message.Body;
    
    let uncachedLocations = [];

    userHistory.forEach((location) => {
      if (cache.llenAsync(`${location}:results`) === 0) {
        uncachedLocations.push(location);
      }
    });

    //Clears the user's cache first, then updates it with latest history
    //Sends one location at a time, for now.

    cache
      .delAsync(userId)
      .then(() => cache.lpushAsync(`${userId}:results`, userHistory))
      .then(() => {
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
            .then(result => console.log('SQS sent to Experiences Q with: ', result))
            .catch(err => console.error('ExperienceQ issue is: ', err));
        })
      })
      .catch(err => console.error('UserQ handleMessage issue is: ', err))
    
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
    console.log('Fetched from the ExperiencesQ: ', message.Body);

    //TODO: conditionally handle the 2 different types of payloads:
    //1) location-experiences
    //2) user-location-experiences pagination

    const { locationId, locations } = message.Body;

    cache
      .lpushAsync(`${locationId}:results`, locations)
      .then(result => console.log('Wrote to location cache: ', result))
      .catch(err => console.error('Issue with location cache: ', err));

    //Intentionally making done non-blocking.

    done();
  },

  //Starting with a long wait time

  waitTimeSeconds: 10
});

experiencesWorker.on('empty', data => console.log('ExperiencesQ is empty...'));
experiencesWorker.on('error', err => console.error('ExpereiencesQ has this issue: ', err));
experiencesWorker.start();