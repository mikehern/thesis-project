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

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

const USER_SERVICE = `javi's ip/route`;
const USER_Q_SEARCHEDLOCATION = `javi's inbound queue`;
const AG_Q_CLICKEVENTS = `vinoj's inbound queue`;
const EXPERIENCES_SERVICE = `aric's ip/route`;


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

//Inspects the cache, decorates each experience with an abtest, keeps the experiences in an array,
//sends the array back to the client with a 200, decorates the payload with a viewed state,
//serially writes each experience into the db.

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
        Promise.mapSeries(dbPayload, (experience) => {
          return dbWrite(experience);
        })
      });
  });
};

//Routes
app.get('/', (req, res) => {
  
  const userKey = req.query.user;
  const userHistory = cache
    .llenAsync(userKey)
    .then(result => result)
    .catch(err => console.error(err));
  
  if (userHistory === 0) {
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

    sqs
      .sendMessage(userSvcPayload).promise()
      .then(result => console.log('SQS sent to User Q with: ', result))
      .catch(err => console.error(err));
  }

  //Serving the client static assets is currently decoupled from pre-fetching and caching

  res.status(200).send(`User landed on homepage via root route at ${TSNow}`);
  console.log(`Client GET at '/' ${TSNow}`);
});



//Strictly a test route.

app.post('/', (req, res) => {
  res.send(`Server received your POST at ${TSNow}`);
  console.log(`POST's payload body is: `, req.body);
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
    .then(result => console.log('SQS sent to Aggregator Q: ', result))
    .catch(err => console.error(err));
});



//Client directly goes to experiences.

app.get('/experiences', (req, res) => {
  const cacheKey = `${req.query.user}:results`;
  let clientPayload = [];
  let dbPayload = [];

  //If user experiences cache contains personalized experiences, run the sendABPayload helper
  //Otherwise run an ABtest and send them a generic set of popular experiences.

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



//Client searches for experiences at a specific location.
      
app.get('/experiences/:location', (req, res) => {
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
            Promise.mapSeries(res.data, (experience) => {
              return cache.lpushAsync(cacheKey, experience);
            })
              .then(() => cache.lrangeAsync(cacheKey, 0, 11))
              .then(reply => sendABPayload(reply, ABResult))
          })
      }
    })

    //...after responding to the client with a 200, places the userId and locationId
    //on the experiences Q. For future pagination. May not need this.

    .then(() => {
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
        .then(result => console.log('SQS sent to Experiences Q with: ', result))
        .catch(err => console.error('ExperienceQ issue is: ', err));
    })
    .catch(err => console.error(err));
});



if (!module.parent) {
  app.listen(port, () => console.log(`Listening on port ${port}`));
}
