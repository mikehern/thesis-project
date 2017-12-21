const express = require('express');
const app = express();
const moment = require('moment');
const port = 1337;
const bodyParser = require('body-parser');
const redis = require('redis');

const Promise = require('bluebird');

const redis = require('redis');
const cache = redis.createClient();
Promise.promisifyAll(redis.RedisClient.prototype);
Promise.promisifyAll(redis.Multi.prototype);
cache.on('connect', () => console.log('Connected to Redis!'));

const cassandra = require('cassandra-driver');
const client = new cassandra.Client({ contactPoints: ['127.0.0.1'], keyspace: 'events' });

const cache = redis.createClient();

cache.on('connect', () => {
  console.log('Connected to Redis server.');
});

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

const TSNow = moment(Date.now()).format('llll');



app.get('/', (req, res) => {
  res.status(200).send(`User landed on homepage via root route at ${TSNow}`);
  console.log(`Client GET at '/' ${TSNow}`);
});

app.post('/', (req, res) => {
  res.send(`Server received your POST at ${TSNow}`);
  console.log(`POST's payload body is: `, req.body);
});

app.post('/events', (req, res) => {
  const {
    event_type: e_type,
    experience_id: x_id,
    experiment_type: ab_type,
    user_id: u_id 
  } = req.body;
  

  //TODO Change db int to largeint
  const query = `INSERT into events.user_events(event_timestamp, event_type, experience_id, experiment_type, user_id) values(${Date.now()}, '${e_type}', ${x_id}, '${ab_type}', ${u_id});`;

  //TODO: after writing to db, chain a promise to add to Aggregators queue
  //http://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/sqs-examples-using-queues.html
  client.execute(query)
    .then(result => console.log('DB was hit with: ', result))
    .then(result => res.status(200).send(`DB completed your post at ${TSNow}`))
    .catch(reason => console.error(reason));
});

app.get('/experiences', (req, res) => {
  //No location specified
    //Look up the user:results LIST
      //If it exists
        //LPOP 12 results
        //Send response to client with these 12
        //Async write a VIEWED event to db
      //If not
        //Flip a coin
        //LRANGE 12 from popularityReview or popularityRating LISTS
        //Send response to client with these 12
        //Async write a VIEWED event to db, along with experiment type
  
  //Location specified
    //Look up the location:results LIST
    //If it exists
      //LRANGE 12 results
      //Flip a coin, and sort by review or rating
      //Decorate each obj with the experiment type
      //Send response to client with these 12
      //Async send the user_id/location pair to the user_searches queue
    //If not
      //Flip a coin
      //Make a GET to experiences: sortBy, location, batch number
      //Use results to populate cache
      //LRANGE 12 from this cache
      //Decorate each obj with the experiment type
      //Send response to client with first 12
      //Async send the user_id/location pair to the user_searches queue
});

if (!module.parent) {
  app.listen(port, () => console.log(`Listening on port ${port}`));
}
