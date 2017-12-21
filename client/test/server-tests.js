const expect = require('chai').expect;
const request = require('request');
const supertest = require('supertest');
const server = supertest('http://localhost:1337');

describe('User arrives on homepage.', () => {

  describe(`Client connects to /`, () => {
    it('Responds with a 200', (done) => {
      server
        .get('/')
        .expect(200, done);
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
          experiment_type: "TEST",
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