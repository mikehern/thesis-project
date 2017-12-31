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

const pino = require('pino')();

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

const USER_SERVICE = `javi's ip/route`;
const USER_Q_SEARCHEDLOCATION = `https://sqs.us-east-1.amazonaws.com/410939018954/Dummy_USER_Q_SEARCHEDLOCATION`;
const AG_Q_CLICKEVENTS = `https://sqs.us-east-1.amazonaws.com/410939018954/Dummy_AG_Q_CLICKEVENTS`;
const EXPERIENCES_SERVICE = `aric's ip/route`;


//Helpers
const TSNow = moment(Date.now()).format('llll');
const coinFlip = () => {
  return (Math.floor(Math.random() * 2) == 0) ? 'reviews' : 'ratings';
};
const dbWrite = (experience, res) => {
  pino.info({ route: '', method: '', stage: 'BEGIN', function: 'dbWrite' });
  const {
    event_type: e_type,
    experience_id: x_id,
    experiment_type: ab_type,
    user_id: u_id
  } = experience;

  const query = `INSERT into events.user_events(event_timestamp, event_type, experience_id, experiment_type, user_id) values(${Date.now()}, '${e_type}', ${x_id}, '${ab_type}', ${u_id});`;

  client.execute(query)
    .then(result => pino.info({ route: '', method: '', stage: 'MIDDLE', function: 'dbWrite' }, 'after db write, before res'))
    .then(() => res.status(200).send(`DB completed your post at ${TSNow}`))
    .then(() => pino.info({ route: '', method: '', stage: 'END', function: 'dbWrite' }))
    .catch(err => pino.error(new Error({ route: '', method: '', stage: 'MIDDLE', function: 'dbWrite' }, 'during dbWrite')));
};

//Inspects the cache, decorates each experience with an abtest, keeps the experiences in an array,
//sends the array back to the client with a 200, decorates the payload with a viewed state,
//serially writes each experience into the db.

const sendABPayload = (cacheReply, ABResult) => {
  pino.info({ route: '', method: '', stage: 'BEGIN', function: 'sendABPayload' });
  let clientPayload = [];
  let dbPayload = [];
  ABResult = ABResult || coinFlip();
  return new Promise((resolve, reject) => {
    resolve(cacheReply => {
      clientPayload = cacheReply.map(el => el.experiment_type = ABResult);
      return clientPayload;
    })
      .then(clientPayload => {
        pino.info({ route: '', method: '', stage: 'MIDDLE', function: 'sendABPayload' }, 'after AB decorate, before res send');
        res.status(200).send(clientPayload);
      })
      .then(() => {
        pino.info({ route: '', method: '', stage: 'MIDDLE', function: 'sendABPayload' }, 'after res send, before db decorate');
        dbPayload = clientPayload.map(el => el.event_type = 'VIEWED');
        return dbPayload;
      })
      .then(dbPayload => {
        pino.info({ route: '', method: '', stage: 'MIDDLE', function: 'sendABPayload' }, 'after db decorate, before promise db write');
        Promise.mapSeries(dbPayload, (experience) => {
          return dbWrite(experience);
        });
      })
      .then(() => pino.info({ route: '', method: '', stage: 'END', function: 'sendABPayload' }))
      .catch(err => pino.error(new Error({ route: '', method: '', stage: 'MIDDLE', function: 'sendABPayload' }, 'during promise')));
  });
};

//Routes
app.get('/:user', (req, res) => {
  pino.info({ route: '/', method: 'GET', stage: 'BEGIN' });
  const userKey = req.query.user;

  cache
    .llenAsync(userKey)
    .then(result => {
      pino.info({ route: '/', method: 'GET', stage: 'MIDDLE' }, 'after cache inspected');
      if (result === 0) {
        const userSvcPayload = {
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
          MessageBody: JSON.stringify(userKey),
          QueueUrl: USER_Q_SEARCHEDLOCATION
        };

        pino.info({ route: '/', method: 'GET', stage: 'MIDDLE' }, 'before sqs sent');

        sqs
          .sendMessage(userSvcPayload).promise()
          .then(result => pino.info({ route: '/', method: 'GET', stage: 'MIDDLE' }, 'after sqs send'))
          .catch(err => pino.error(new Error({ route: '/', method: 'GET', stage: 'MIDDLE' }, 'after sqs send')));
      }
    })
    .catch(err => pino.error(new Error({ route: '/', method: 'GET', stage: 'MIDDLE' }, 'after cache and sqs send')));
  


  //Serving the client static assets is currently decoupled from pre-fetching and caching

  res.status(200).send(`User landed on homepage via root route at ${TSNow}`);
  pino.info({ route: '/', method: 'GET', stage: 'END' });

});



//Strictly a test route.

app.post('/', (req, res) => {
  pino.info({ route: '/', method: 'POST', stage: 'BEGIN' });
  res.status(201).send(`Server received your POST at ${TSNow}`);
  pino.info({ route: '/', method: 'POST', stage: 'END' });
});



//Handles client clicks.

app.post('/events', (req, res) => {
  pino.info({ route: '/events', method: 'POST', stage: 'BEGIN' });
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
    QueueUrl: AG_Q_CLICKEVENTS
  };
 
  const sendMessageAsync = sqs.sendMessage(params).promise();

  //Take the click event, place it into a promised db write,
  //then push the message to the Aggregator queue.

  return new Promise((resolve, reject) => {
    pino.info({ route: '/events', method: 'POST', stage: 'MIDDLE' }, 'before db write');
    resolve(dbWrite(body, res));
  })
    .then(() => pino.info({ route: '/events', method: 'POST', stage: 'MIDDLE' }, 'after db write, before SQS send'))
    .then(() => sendMessageAsync)
    .then(() => pino.info({ route: '/events', method: 'POST', stage: 'END' }))
    .catch(err => pino.error(new Error({ route: '/events', method: 'POST', stage: 'MIDDLE' }, 'after db write and sqs send')));
});



//Client directly goes to experiences.

app.get('/experiences', (req, res) => {
  pino.info({ route: '/experiences', method: 'GET', stage: 'BEGIN' });
  const cacheKey = `${req.query.user}:results`;
  let clientPayload = [];
  let dbPayload = [];

  //If user experiences cache contains personalized experiences, run the sendABPayload helper
  //Otherwise run an ABtest and send them a generic set of popular experiences.

  pino.info({ route: '/experiences', method: 'GET', stage: 'MIDDLE' }, 'before inspecting cache');
  cache.lpopAsync(cacheKey, 0, 11)
    .then(reply => {
      if (reply.length !== 0) {
        pino.info({ route: '/experiences', method: 'GET', stage: 'MIDDLE' }, 'after cache verified, before sendAB');
        return sendABPayload(reply);
      } else {
        pino.info({ route: '/experiences', method: 'GET', stage: 'MIDDLE' }, 'after cache non-exist, before sendAB default cache');
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
    .then(() => pino.info({ route: '/experiences', method: 'GET', stage: 'END' }))
    .catch(err => pino.error(new Error({ route: '/experiences', method: 'GET', stage: 'MIDDLE' }, 'during cache or sendAB')));
});



//Client searches for experiences at a specific location.
      
app.get('/experiences/:location', (req, res) => {
  pino.info({ route: '/experiences:location', method: 'GET', stage: 'BEGIN' });
  const cacheKey = `${req.query.location}:results`;
  let clientPayload = [];
  let dbPayload = [];

  //If location cache contains experiences, run the sendABPayload helper
  //Otherwise GET listings from the experiences service,
  //serially write them to the cache,
  //take the first 12 results from the cache,
  //and call the sendABPayload helper.

  cache.lrangeAsync(cacheKey, 0, 11)
    .then(reply => {
      pino.info({ route: '/experiences:location', method: 'GET', stage: 'MIDDLE' }, 'cache inspected');
      if (reply.length !== 0) {
        pino.info({ route: '/experiences:location', method: 'GET', stage: 'MIDDLE' }, 'cache inspected, before sendAB');
        return sendABPayload(reply);
      } else {
        const ABResult = coinFlip();
        pino.info({ route: '/experiences:location', method: 'GET', stage: 'MIDDLE' }, 'cache inspected, before GET to expsvc');
        return axios.get(EXPERIENCES_SERVICE, {
          params: {
            location_id: req.query.location,
            sort_order: ABResult,
            batch: 1
          }
        })
        //Starting serially... want to test promise.all concurrency later
          .then(res => {
            pino.info({ route: '/experiences:location', method: 'GET', stage: 'MIDDLE' }, 'after GET to expsvc, before cache write');
            Promise.mapSeries(res.data, (experience) => {
              return cache.lpushAsync(cacheKey, experience);
            })
              .then(() => {
                pino.info({ route: '/experiences:location', method: 'GET', stage: 'MIDDLE' }, 'after cache write, before cache pull');
                cache.lrangeAsync(cacheKey, 0, 11);
              })
              .then(reply => {
                pino.info({ route: '/experiences:location', method: 'GET', stage: 'MIDDLE' }, 'after promise cache, before sendAB call');
                sendABPayload(reply, ABResult);
              })
              .catch(err => pino.error(new Error({ route: '/experiences:location', method: 'GET', stage: 'MIDDLE' }, 'during cache')));
          })
          .catch(err => pino.error(new Error({ route: '/experiences:location', method: 'GET', stage: 'MIDDLE' }, 'after get to expsvc')));
      }
    })

    //...after responding to the client with a 200, places the userId and locationId
    //on the experiences Q. For future pagination. May not need this.

    .then(() => {
      pino.info({ route: '/experiences:location', method: 'GET', stage: 'MIDDLE' }, 'after sendAB call, before SQS send');
      const userSearchPayload = {
        user_id: `${req.query.user}`,
        location_id: req.query.location
      };

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
        MessageBody: JSON.stringify(userSearchPayload),
        QueueUrl: EXPERIENCES_Q_INBOUND
      };

      sqs.sendMessage(params).promise()
        .then(() => pino.info({ route: '/experiences:location', method: 'GET', stage: 'MIDDLE' }, 'after sqs send'))
        .catch(err => pino.error(new Error({ route: '/experiences:location', method: 'GET', stage: 'MIDDLE' }, 'during SQS send')));
    })
    .then(() => pino.info({ route: '/experiences:location', method: 'GET', stage: 'END' }))
    .catch(err => pino.error(new Error({ route: '/experiences:location', method: 'GET', stage: 'MIDDLE' }, 'SQS prep')));
});



if (!module.parent) {
  app.listen(port, () => console.log(`Listening on port ${port}`));
}
