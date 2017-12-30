const expect = require('chai').expect;
const request = require('request');
const supertest = require('supertest');
const server = supertest('http://localhost:1337');
const fakedata = require('./fake-data');
const sinon = require('sinon');

const Promise = require('bluebird');
const redis = require('redis');
const cache = redis.createClient();
Promise.promisifyAll(redis.RedisClient.prototype);
Promise.promisifyAll(redis.Multi.prototype);

const cassandra = require('cassandra-driver');
const client = new cassandra.Client({
  contactPoints: ['127.0.0.1'],
  keyspace: 'events'
});

const perfectUserId = '3011352300';
const perfectUserHistory = [2124028216, -1681810253, -1247473455, -365845894, -1560504360];
const perfectGetReq = `user_id=${perfectUserId}`;
const perfectSearchResults = {
  "userId": "1000",
  "recent": perfectUserHistory
};
const perfectUserKey = `3011352300:results`;


describe('User arrives on homepage.', () => {

  before(done => {
    console.log('beginning before hook...');
    cache
      .lpushAsync(perfectUserKey, perfectUserHistory)
      .then(result => console.log('beforepush:', result))
      .then(() => cache.lrangeAsync(perfectUserKey, 0, -1))
      .then(result => console.log('afterpush:', result))
      .then(() => done());
  });

  after(done => {

    console.log('beginning after hook...');
    cache
      .lrangeAsync(perfectUserKey, 0, -1)
      .then(result => console.log('beforedelete: ', result))
      .then(() => cache.delAsync(perfectUserKey))
      .then(result => console.log('afterdelete:', result))
      .then(() => cache.lrangeAsync(perfectUserKey, 0, -1))
      .then(result => console.log('lastly...', result))
      .then(() => done())
      .catch(err => console.error(err));
    
  });

  describe(`Client connects to /`, () => {
    it('Responds with a 200', (done) => {
      server
        .get('/')
        .expect(200, done);
    });
  });
  
  describe(`Prepares pre-fetched results in case the user clicks on Experiences.`, () => {
    xit(`Does not create a payload when personalized results are already cached.`, () => {

    });
    xit(`Creates a payload with the user id as a string.`, () => {

    });
    xit(`User search history queue contains the payload.`, () => {

    });
    xit(`Async expects a User-search history pair in the User search results queue`, () => {

    });
  });

  describe(`Builds a cached default result set based on past search history.`, () => {
    xit(`Populates the default set with any locations already cached.`, () => {

    });
    xit(`Creates a payload of a location id for each uncached search history location.`, () => {

    });
    xit(`Experiences inbound queue contains a payload for each location.`, () => {

    });
    xit(`Async expects 36 experiences for each location in the Experiences results queue.`, () => {

    });
    xit(`Async updates the cache for each location-experience payload received.`, () => {

    });
    xit(`Async updates the default set with the experiences payload.`, () => {

    });
  });

  describe(`Contains pre-fetched sets in case users have no search history.`, () => {
    xit(`Contains a cached set of 36 experiences sorted by rating.`, () => {
      
    });
    xit(`Contains a cached set of 36 experiences sorted by price.`, () => {

    });
  });

});





describe('User goes to Experiences product.', () => {

  describe(`Client GETS to /experiences`, () => {
    xit('Responds with a 200', (done) => {
      server
        .get('/experiences')
        .expect(200, done);
    });

    xit('Is served a default set of 12 results', () => {

    });

    xit('Is served results specific to a location', () => {

    });

    xit('Listing payload includes sorted-by-ratings or price', () => {

    });
  });

  describe(`DB includes written event`, () => {
    xit(`Rows are created for all served results`, () => {

    });

    xit(`A record includes 'VIEWED' for a served result.`, () => {

    });
  });
});





describe('User clicks on an Experiences item.', () => {

  describe('Client POSTS to our server.', () => {
    it('Connects to the server', (done) => {
      server
        .post('/events')
        .send({
          event_type: "CLICK",
          experience_id: 999999999,
          experiment_type: "XMASSTEST2",
          user_id: 111111111
        })
        .expect(200, done);
    });

  });

  describe('DB includes written event', () => {
    xit('Row contains original payload', () => {

    });
    xit('Row includes timestamp', () => {

    });
  });

  describe('Aggregator receives event', () => {
    xit(`Aggregator's queue includes the original payload`, () => {

    });
  });
});

