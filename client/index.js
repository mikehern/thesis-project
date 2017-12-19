const express = require('express');
const app = express();
const moment = require('moment');
const port = 1337;
const bodyParser = require('body-parser');

const cassandra = require('cassandra-driver');
//TODO: use proper keyspace
const client = new cassandra.Client({ contactPoints: ['127.0.0.1'], keyspace: '' });

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

const TSNow = moment(Date.now()).format('llll');

app.get('/', (req, res) => {
  res.send(`User landed on homepage via root route at ${TSNow}`);
  console.log(`Client GET at '/' ${TSNow}`);
});

app.post('/', (req, res) => {
  res.send(`Server received your POST at ${TSNow}`);
  console.log(`POST's payload body is: `, req.body);
});

app.post('/events', (req, res) => {
  const { event_type: e_type, experience_id: x_id, experiment_type: ab_type, user_id: u_id } = req.body;
  
  //TODO: use proper keyspace
  const query = `insert into events2.events(event_timestamp, event_type, experience_id, experiment_type, user_id) values(${Date.now()}, '${e_type}', ${x_id}, '${ab_type}', ${u_id});`;

  client.execute(query)
    .then(result => console.log('DB was hit with: ', result))
    .then(result => res.status(200).send(`DB completed your post at ${TSNow}`))
    .catch(reason => console.error(reason));
});

app.listen(port, () => console.log(`Listening on port ${port}`));