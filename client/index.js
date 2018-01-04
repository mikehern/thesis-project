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
  return (Math.floor(Math.random() * 2) == 0) ? 'prices' : 'ratings';
};
const logms = (tuple) => (tuple[0] * 1000000000 + tuple[1]) / 1000000;

const dbWrite = (experience, res) => {
  const {
    event_type: e_type,
    experience_id: x_id,
    experiment_type: ab_type,
    user_id: u_id
  } = experience;

  const query = `INSERT into events.user_events(event_timestamp, event_type, experience_id, experiment_type, user_id) values(${Date.now()}, '${e_type}', ${x_id}, '${ab_type}', ${u_id});`;

  client.execute(query)
    .then(() => res.status(200).send(`DB completed your post at ${TSNow}`))
    .catch(err => pino.error(new Error({ route: '', method: '', stage: 'MIDDLE', function: 'dbWrite' }, 'during dbWrite')));
};

const dbWriteEx = (experience, res) => {
  const {
    event_type: e_type,
    experience_id: x_id,
    experiment_type: ab_type,
    user_id: u_id
  } = experience;

  const query = `INSERT into events.user_events(event_timestamp, event_type, experience_id, experiment_type, user_id) values(${Date.now()}, '${e_type}', ${x_id}, '${ab_type}', ${u_id});`;

  client.execute(query)
    .then(() => res.status(200).send(`DB completed your post at ${TSNow}`))
    .catch(err => pino.error(new Error({ route: '', method: '', stage: 'MIDDLE', function: 'dbWrite' }, 'during dbWrite')));
};

//Inspects the cache, decorates each experience with an abtest, keeps the experiences in an array,
//sends the array back to the client with a 200, decorates the payload with a viewed state,
//serially writes each experience into the db.

const sendABPayload = (cacheReply, ABResult, res, userId) => {
  let clientPayload = [];
  let dbPayload = [];
  ABResult = ABResult || coinFlip();

  cacheReply.forEach(el => {
    let parsed = JSON.parse(el);
    parsed.experiment_type = ABResult;
    clientPayload.push(parsed);
  });


  //TODO: take subset of dbpayload and pass into dbWrite. Refactor both transforms into 1 then block.

  Promise.resolve(res.status(200).send(clientPayload))
    .then(() => {
      clientPayload.forEach(el => {
        let dbFormatted = {
          event_type: 'VIEWED',
          experience_id: Number(el.experience_id),
          experiment_type: el.experiment_type,
          user_id: Number(userId)
        };
        dbPayload.push(dbFormatted);
      });
      return dbPayload;
    })
    .then(dbPayload => {
      Promise.map(dbPayload, (experience) => {
        return dbWriteEx(experience);
      })
    })
    .catch(err => pino.error(new Error({ route: '', method: '', stage: 'MIDDLE', function: 'sendABPayload' }, 'during promise')));
    

  
  // return new Promise((resolve, reject) => {
  //   resolve(cacheReply => {
  //     console.log('HERES CLIENTPAYLOAD: ', clientPayload);
  //     clientPayload = cacheReply.map(el => el.experiment_type = ABResult);
  //     return clientPayload;
  //   })
  //     .then(clientPayload => {
  //       pino.info({ route: '', method: '', stage: 'MIDDLE', function: 'sendABPayload' }, 'after AB decorate, before res send');
  //       res.status(200).send(clientPayload);
  //     })
  //     .then(() => {
  //       pino.info({ route: '', method: '', stage: 'MIDDLE', function: 'sendABPayload' }, 'after res send, before db decorate');
  //       dbPayload = clientPayload.map(el => el.event_type = 'VIEWED');
  //       return dbPayload;
  //     })
  //     .then(dbPayload => {
  //       pino.info({ route: '', method: '', stage: 'MIDDLE', function: 'sendABPayload' }, 'after db decorate, before promise db write');
  //       Promise.mapSeries(dbPayload, (experience) => {
  //         return dbWrite(experience);
  //       });
  //     })
  //     .then(() => pino.info({ route: '', method: '', stage: 'END', function: 'sendABPayload' }))
  //     .catch(err => pino.error(new Error({ route: '', method: '', stage: 'MIDDLE', function: 'sendABPayload' }, 'during promise')));
    
  //   reject(console.error(err));
  // });
};

//Routes
app.get('/home', (req, res) => {
  pino.info({ route: '/home', method: 'GET', stage: 'BEGIN' });
  const userKey = req.query.user;

  cache
    .llenAsync(userKey)
    .then(result => {
      pino.info({ route: '/home', method: 'GET', stage: 'MIDDLE' }, 'after cache inspected');
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

        pino.info({ route: '/home', method: 'GET', stage: 'MIDDLE' }, 'before sqs sent');

        sqs
          .sendMessage(userSvcPayload).promise()
          .then(result => pino.info({ route: '/home', method: 'GET', stage: 'MIDDLE' }, 'after sqs send'))
          .catch(err => pino.error(new Error({ route: '/home', method: 'GET', stage: 'MIDDLE' }, 'after sqs send')));
      }
    })
    .catch(err => pino.error(new Error({ route: '/home', method: 'GET', stage: 'MIDDLE' }, 'after cache and sqs send')));
  


  //Serving the client static assets is currently decoupled from pre-fetching and caching

  res.status(200).send(`User landed on homepage via root route at ${TSNow}`);
  pino.info({ route: '/home', method: 'GET', stage: 'END' });

});



//Strictly a test route.

app.post('/', (req, res) => {
  pino.info({ route: '/', method: 'POST', stage: 'BEGIN' });
  res.status(201).send(`Server received your POST at ${TSNow}`);
  pino.info({ route: '/', method: 'POST', stage: 'END' });
});



//Handles client clicks.

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
    QueueUrl: AG_Q_CLICKEVENTS
  };
 
  const sendMessageAsync = sqs.sendMessage(params).promise();

  //Take the click event, place it into a promised db write,
  //then push the message to the Aggregator queue.

  return new Promise((resolve, reject) => {
    resolve(dbWrite(body, res));
  })
    .then(() => sendMessageAsync)
    .catch(err => pino.error(new Error({ route: '/events', method: 'POST', stage: 'MIDDLE' }, 'after db write and sqs send')));
});



//Client directly goes to experiences.

app.get('/experiences', (req, res) => {
  const routeBegin = process.hrtime();
  pino.info({ route: '/experiences', method: 'GET', stage: 'BEGIN' });
  const cacheKey = `${req.query.user}:results`;
  let clientPayload = [];
  let dbPayload = [];

  //If user experiences cache contains personalized experiences, run the sendABPayload helper
  //Otherwise run an ABtest and send them a generic set of popular experiences.

  pino.info({ route: '/experiences', method: 'GET', stage: 'MIDDLE' }, 'before inspecting cache');
  cache.lrangeAsync(cacheKey, 0, 11)
    .then(reply => {
      if (reply.length !== 0) {
        const notCached = process.hrtime(routeBegin);
        pino.info({ route: '/experiences', method: 'GET', stage: 'MIDDLE', duration: `${logms(notCached)}` }, 'after cache verified, before sendAB');
        return sendABPayload(reply);
      } else {
        pino.info({ route: '/experiences', method: 'GET', stage: 'MIDDLE' }, 'after cache non-exist, before sendAB default cache');
        const ABResult = coinFlip();
        const coinFlipped = process.hrtime(routeBegin);
        if (ABResult === 'ratings') {
          cache
            .lrangeAsync('popularRatings', 0, 11)
            .then(reply => {
              const ratingsCache = process.hrtime(routeBegin);
              pino.info({ route: '/experiences', method: 'GET', stage: 'MIDDLE', duration: `${logms(ratingsCache)}` }, 'RATINGSCACHED');
              sendABPayload(reply, null, res, req.query.user);
            });
        } else if (ABResult === 'prices') {
          cache
            .lrangeAsync('popularPrices', 0, 11)
            .then(reply => {
              const pricesCache = process.hrtime(routeBegin);
              pino.info({ route: '/experiences', method: 'GET', stage: 'MIDDLE', duration: `${logms(pricesCache)}` }, 'PRICESCACHED');
              sendABPayload(reply, null, res, req.query.user);
            });
        }
      }
    })
    .then(() => {
      const completedRoute = process.hrtime(routeBegin);
      pino.info({ route: '/experiences', method: 'GET', stage: 'END', duration: `${logms(completedRoute)}` });
    })
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
