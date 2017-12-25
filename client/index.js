const express = require('express');
const app = express();
const moment = require('moment');
const port = 1337;
const bodyParser = require('body-parser');
const axios = require('axios');

const AWS = require('aws-sdk');
const AWScredentials = require('./config');
const queueUrl = 'https://sqs.us-east-1.amazonaws.com/410939018954/ToyQ';
AWS.config.update({
  region: AWScredentials.sqsConfig.region,
  accessKeyId: AWScredentials.sqsConfig.accessKeyId,
  secretAccessKey: AWScredentials.sqsConfig.secretAccessKey
});
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });
const Consumer = require('sqs-consumer');
const worker = Consumer.create({
  queueUrl: queueUrl,
  handleMessage: (message, done) => {
    console.log('Q payloads body is: ', message.Body);
    done();
  },
  waitTimeSeconds: 10
});
worker.on('empty', data => console.log('Queue is empty...'));
worker.on('error', err => console.error(err));
worker.start();

const Promise = require('bluebird');

const redis = require('redis');
const cache = redis.createClient();
Promise.promisifyAll(redis.RedisClient.prototype);
Promise.promisifyAll(redis.Multi.prototype);
cache.on('connect', () => console.log('Connected to Redis!'));

const cassandra = require('cassandra-driver');
const client = new cassandra.Client({
  contactPoints: ['127.0.0.1'],
  keyspace: 'events' 
});

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

const USER_SERVICE = `javi's ip/route`;
const USER_Q_SEARCHEDLOCATION = `javi's inbound queue`;
const USER_Q_SEARCHRESULTS = `javi's outbound queue`;
const AG_SERVICE = `vinoj's ip/route`;
const AG_Q_CLICKEVENTS = `vinoj's inbound queue`;
const EXPERIENCES_SERVICE = `aric's ip/route`;
const EXPERIENCES_Q_UPDATES = `aric's outbound queue`;


//Helpers
const TSNow = moment(Date.now()).format('llll');
const coinFlip = () => {
  return (Math.floor(Math.random() * 2) == 0) ? 'reviews' : 'ratings';
};
const dbWrite = (experience, res) => {
  const {
    event_type: e_type,
    experience_id: x_id,
    experiment_type: ab_type,
    user_id: u_id
  } = experience;

  const query = `INSERT into events.user_events(event_timestamp, event_type, experience_id, experiment_type, user_id) values(${Date.now()}, '${e_type}', ${x_id}, '${ab_type}', ${u_id});`;

  client.execute(query)
    .then(result => console.log('DB was hit with: ', result))
    .then(result => res.status(200).send(`DB completed your post at ${TSNow}`))
    .catch(reason => console.error(reason));
};

const sendABPayload = (cacheReply, ABResult) => {
  let clientPayload = [];
  let dbPayload = [];
  ABResult = ABResult || coinFlip();
  return new Promise((resolve, reject) => {
    resolve(cacheReply => {
      clientPayload = cacheReply.map(el => el.experiment_type = ABResult);
      return clientPayload;
    })
      .then(clientPayload => res.status(200).send(clientPayload))
      .then(() => {
        dbPayload = clientPayload.map(el => el.event_type = 'VIEWED');
        return dbPayload;
      })
      .then(dbPayload => {
        Promise.map(dbPayload, (experience) => {
          return dbWrite(experience);
        })
      });
  });
};

//Routes
app.get('/', (req, res) => {
  res.status(200).send(`User landed on homepage via root route at ${TSNow}`);
  console.log(`Client GET at '/' ${TSNow}`);
});

app.post('/', (req, res) => {
  res.send(`Server received your POST at ${TSNow}`);
  console.log(`POST's payload body is: `, req.body);
});

app.post('/events', (req, res) => {
  let body = req.body;
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
    MessageBody: JSON.stringify(body),
    QueueUrl: queueUrl
  };
  const sendMessageAsync = sqs.sendMessage(params).promise();
  return new Promise((resolve, reject) => {
    resolve(dbWrite(body, res));
  })
    .then(() => sqs.sendMessage())
    .then(result => console.log('SQS sent with: ', result))
    .catch(err => console.error(err));
});

app.get('/experiences', (req, res) => {
  const cacheKey = `${req.query.user}:results`;
  let clientPayload = [];
  let dbPayload = [];

  cache.lpopAsync(cacheKey, 0, 11)
    .then(reply => {
      if (reply.length !== 0) {
        return sendABPayload(reply);
      } else {
        const ABResult = coinFlip();
        if (ABResult === 'ratings') {
          cache
            .lrangeAsync(popularRatings, 0, 11)
            .then(reply => sendABPayload(reply))
        } else if (ABResult === 'reviews') {
          cache
            .lrangeAsync(popularReviews, 0, 11)
            .then(reply => sendABPayload(reply))
        }
      }
    })
    .catch(err => console.error(err));
});
      
app.get('/experiences/:location', (req, res) => {
  const cacheKey = `${req.query.location}:results`;
  let clientPayload = [];
  let dbPayload = [];

  cache.lrangeAsync(cacheKey, 0, 11)
    .then(reply => {
      if (reply.length !== 0) {
        return sendABPayload(reply);
      } else {
        const ABResult = coinFlip();
        return axios.get(EXPERIENCES_SERVICE, {
          params: {
            location_id: req.query.location,
            sort_order: ABResult,
            batch: 1
          }
        })
        //Starting serially... want to test promise.all concurrency later
          .then(res => {
            Promise.map(res.data, (experience) => {
              return cache.lpushAsync(cacheKey, experience);
            })
              .then(() => cache.lrangeAsync(cacheKey, 0, 11))
              .then(reply => sendABPayload(reply, ABResult))
          })
      }
    })
    .then(() => {
      const userSearchPayload = {
        user_id: `${req.query.user}`,
        location_id: req.query.location
      }
      sqs.sendMessage(USER_Q_SEARCHEDLOCATION, userSearchPayload);
    })
    .catch(err => console.error(err));
});



if (!module.parent) {
  app.listen(port, () => console.log(`Listening on port ${port}`));
}
